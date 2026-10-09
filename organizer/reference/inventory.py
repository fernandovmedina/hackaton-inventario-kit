"""Solución de referencia (SOLO organizadores): valida que las pruebas de contrato se
pueden pasar y que dicen lo que el contrato dice.

Deliberadamente simple: estado en memoria reconstruido desde los tópicos al arrancar,
un solo hilo consumidor y un candado compartido con la API (así no hay reservas dobles).
Las operaciones de piso (salidas, transferencias, conteos) no se persisten: en un reinicio
se pierden. Una solución real las guarda.

    uv run --with confluent-kafka organizer/reference/inventory.py --broker localhost:19092 --port 8000
"""

from __future__ import annotations

import argparse
import json
import re
import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition

INPUT_TOPICS = ["shop.catalog", "shop.work_orders", "shop.inspections", "shop.purchasing"]
RECEIVING = 100


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def norm_sku(value: str | None) -> str:
    return re.sub(r"\s+", "", (value or "")).upper()


class Deferred(Exception):
    """Falta algo que todavía no llega (orden, pieza, línea): reintentar después."""


class HttpError(Exception):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status, self.detail = status, detail


@dataclass
class Need:
    wo_id: int
    bom_line_id: int
    part_id: int | None
    name: str
    inspection_item_id: int
    inspection_id: int
    required: int | None
    seq: int
    reserved: int = 0
    issued: int = 0
    opened_at: str = field(default_factory=now_iso)

    @property
    def missing(self) -> int | None:
        if self.required is None:
            return None
        return max(0, self.required - self.issued - self.reserved)


class Inventory:
    def __init__(self, producer: Producer) -> None:
        self.producer = producer
        self.lock = threading.RLock()
        self.seen: set[str] = set()
        self.pending: list[dict] = []
        self.parts: dict[int, dict] = {}
        self.locations: dict[int, dict] = {}
        self.boms: dict[int, dict[int, dict]] = {}
        self.orders: dict[int, dict] = {}
        self.order_by_code: dict[str, int] = {}
        self.on_hand: dict[tuple[int, int], int] = defaultdict(int)
        self.movements: list[dict] = []
        self.needs: dict[tuple[int, int], Need] = {}
        self.need_seq = 0
        self.unmatched: dict[int, dict] = {}
        self.last_suggestion: dict[int, tuple] = {}

    # --- salidas ---
    def emit(self, event_type: str, key: str, data: dict) -> None:
        event = {"event_id": str(uuid.uuid4()), "event_type": event_type, "event_version": 1,
                 "occurred_at": now_iso(), "source": "inventory", "key": key, "data": data}
        self.producer.produce("inventory.events", key=key.encode(), value=json.dumps(event).encode())
        self.producer.poll(0)

    def code(self, wo_id: int) -> str:
        return self.orders[wo_id]["code"]

    # --- saldos ---
    def part_on_hand(self, part_id: int) -> int:
        return sum(q for (p, _), q in self.on_hand.items() if p == part_id)

    def part_reserved(self, part_id: int) -> int:
        return sum(n.reserved for n in self.needs.values() if n.part_id == part_id)

    def available(self, part_id: int) -> int:
        return self.part_on_hand(part_id) - self.part_reserved(part_id)

    def move(self, mtype: str, part_id: int, location_id: int, qty: int, reference: str | None) -> dict:
        self.on_hand[(part_id, location_id)] += qty
        movement = {"movement_id": len(self.movements) + 1, "type": mtype, "part_id": part_id,
                    "location_id": location_id, "quantity": qty, "occurred_at": now_iso(), "reference": reference}
        self.movements.append(movement)
        return movement

    # --- consumo ---
    def handle(self, event: dict) -> None:
        with self.lock:
            if event["event_id"] in self.seen:
                return
            try:
                self.apply(event)
            except Deferred:
                self.pending.append(event)
                return
            self.seen.add(event["event_id"])
            self.retry_pending()

    def retry_pending(self) -> None:
        progress = True
        while progress and self.pending:
            progress = False
            for event in list(self.pending):
                if event["event_id"] in self.seen:
                    self.pending.remove(event)
                    continue
                try:
                    self.apply(event)
                except Deferred:
                    continue
                self.pending.remove(event)
                self.seen.add(event["event_id"])
                progress = True

    def apply(self, event: dict) -> None:
        t, d = event["event_type"], event["data"]
        if t == "part.upserted":
            self.parts[d["part_id"]] = d
        elif t == "location.upserted":
            self.locations[d["location_id"]] = d
        elif t == "bom.upserted":
            self.boms[d["model_id"]] = {ln["bom_line_id"]: ln for ln in d["lines"]}
        elif t == "work_order.opened":
            self.orders[d["work_order_id"]] = {"code": d["code"], "model_id": d["model_id"], "deleted": False}
            self.order_by_code[d["code"]] = d["work_order_id"]
        elif t == "work_order.deleted":
            if d["work_order_id"] not in self.orders:
                raise Deferred()
            self.orders[d["work_order_id"]]["deleted"] = True
            self.release([k for k, n in self.needs.items() if n.wo_id == d["work_order_id"]])
        elif t == "inspection.approved":
            self.approve(d)
        elif t == "inspection.voided":
            self.release([k for k, n in self.needs.items() if n.inspection_id == d["inspection_id"]])
        elif t == "purchase.item_received":
            self.receive(d)
        # stage_changed, submitted, rejected y discarded no mueven inventario.

    def approve(self, d: dict) -> None:
        wo = self.orders.get(d["work_order_id"])
        if wo is None:
            raise Deferred()
        if wo["deleted"]:
            return
        buys = [it for it in d["items"] if it["action"] == "buy"]
        for it in buys:
            if it["part_id"] is not None and it["part_id"] not in self.parts:
                raise Deferred()
            if it["quantity"] is None and it["bom_line_id"] not in self.boms.get(wo["model_id"], {}):
                raise Deferred()
        for it in buys:
            qty = it["quantity"]
            if qty is None:
                qty = self.boms[wo["model_id"]][it["bom_line_id"]]["qty_per_unit"]
            self.set_need(d["work_order_id"], d["inspection_id"], it, qty)
        self.refresh_suggestions()

    def set_need(self, wo_id: int, inspection_id: int, item: dict, required: int | None) -> None:
        key = (wo_id, item["bom_line_id"])
        need = self.needs.get(key)
        if need is None:
            self.need_seq += 1
            need = Need(wo_id, item["bom_line_id"], item["part_id"], item["name"], item["inspection_item_id"],
                        inspection_id, required, self.need_seq)
            self.needs[key] = need
        else:
            need.required, need.inspection_id, need.inspection_item_id = required, inspection_id, item["inspection_item_id"]
        if need.part_id is not None and required is not None:
            target = max(0, required - need.issued)
            if need.reserved > target:
                need.reserved = target  # baja la necesidad: libera el excedente
            take = min(max(0, self.available(need.part_id)), target - need.reserved)
            if take > 0:
                need.reserved += take
                self.emit("stock.reserved", self.code(wo_id), {
                    "reservation_id": f"{wo_id}-{need.bom_line_id}-{len(self.movements)}-{need.reserved}",
                    "work_order_id": wo_id, "part_id": need.part_id, "quantity": take,
                    "inspection_item_id": need.inspection_item_id})
        if need.missing is None or need.missing > 0:
            self.emit("stock.shortage_detected", self.code(wo_id), {
                "work_order_id": wo_id, "part_id": need.part_id, "name": need.name,
                "missing_quantity": need.missing, "inspection_item_id": need.inspection_item_id})

    def receive(self, d: dict) -> None:
        if d["purpose"] != "restock":
            return
        matches = [pid for pid, p in self.parts.items() if p["sku"] and norm_sku(p["sku"]) == norm_sku(d["part_number"])]
        if len(matches) != 1:
            self.unmatched[d["line_id"]] = {**d, "resolved": False}
            self.emit("stock.unmatched_receipt", f"purchase_line:{d['line_id']}", {
                "purchase_line_id": d["line_id"], "part_number": d["part_number"],
                "description": d["description"], "quantity": d["quantity"]})
            return
        self.stock_in(matches[0], RECEIVING, d)

    def stock_in(self, part_id: int, location_id: int, d: dict) -> dict:
        movement = self.move("receipt", part_id, location_id, d["quantity"], f"compra línea {d['line_id']}")
        self.emit("stock.received", f"part:{part_id}", {
            "part_id": part_id, "location_id": location_id, "quantity": d["quantity"],
            "unit_cost": d["unit_price"], "currency": d["currency"], "purchase_line_id": d["line_id"]})
        self.allocate(part_id, self.order_by_code.get(d.get("work_order_code") or ""))
        self.refresh_suggestions()
        return movement

    def allocate(self, part_id: int, preferred: int | None = None) -> None:
        """Lo disponible surte faltantes: primero la orden preferida, luego el más antiguo."""
        waiting = sorted((n for n in self.needs.values() if n.part_id == part_id and n.missing),
                         key=lambda n: (n.wo_id != preferred, n.seq))
        for need in waiting:
            take = min(self.available(part_id), need.missing)
            if take <= 0:
                break
            need.reserved += take
            code = self.code(need.wo_id)
            self.emit("stock.reserved", code, {
                "reservation_id": f"{need.wo_id}-{need.bom_line_id}-{len(self.movements)}-{need.reserved}",
                "work_order_id": need.wo_id, "part_id": part_id, "quantity": take,
                "inspection_item_id": need.inspection_item_id})
            self.emit("stock.shortage_resolved", code, {"work_order_id": need.wo_id, "part_id": part_id, "quantity": take})

    def release(self, keys: list) -> None:
        """Libera reservas y cierra faltantes; lo liberado surte a quien espera."""
        parts = {self.needs[k].part_id for k in keys if self.needs[k].part_id is not None}
        for key in keys:
            del self.needs[key]
        for part_id in sorted(parts):
            self.allocate(part_id)
        self.refresh_suggestions()

    def refresh_suggestions(self) -> None:
        missing: dict[int, int] = defaultdict(int)
        orders: dict[int, set] = defaultdict(set)
        for n in self.needs.values():
            if n.part_id is not None and n.missing:
                missing[n.part_id] += n.missing
                orders[n.part_id].add(n.wo_id)
        for part_id, qty in missing.items():
            state = (qty, tuple(sorted(orders[part_id])))
            if self.last_suggestion.get(part_id) != state:
                self.last_suggestion[part_id] = state
                self.emit("stock.reorder_suggested", f"part:{part_id}", {
                    "part_id": part_id, "suggested_quantity": qty, "work_order_ids": list(state[1])})
        for part_id in [p for p in self.last_suggestion if p not in missing]:
            del self.last_suggestion[part_id]

    # --- operaciones de piso (API) ---
    def issue(self, body: dict) -> dict:
        wo_id = self.order_by_code.get(body["work_order_code"])
        part_id, loc, qty = body["part_id"], body["location_id"], int(body["quantity"])
        if wo_id is None or self.orders[wo_id]["deleted"] or part_id not in self.parts:
            raise HttpError(404, "orden o pieza desconocida")
        own = [n for n in self.needs.values() if n.wo_id == wo_id and n.part_id == part_id]
        from_reserve = min(qty, sum(n.reserved for n in own))
        if self.on_hand[(part_id, loc)] < qty or qty - from_reserve > self.available(part_id):
            raise HttpError(409, "no alcanza el saldo")
        movement = self.move("issue", part_id, loc, -qty, body["work_order_code"])
        remaining = qty
        for n in sorted(own, key=lambda n: n.seq):
            used = min(remaining, n.reserved)
            n.reserved -= used
            n.issued += used
            remaining -= used
        for n in sorted(own, key=lambda n: n.seq):  # lo que no estaba reservado también cuenta como entregado
            if remaining <= 0:
                break
            extra = min(remaining, n.missing or 0)
            n.issued += extra
            remaining -= extra
        self.emit("stock.issued", body["work_order_code"], {
            "work_order_id": wo_id, "part_id": part_id, "location_id": loc, "quantity": qty, "issued_by": body["issued_by"]})
        self.refresh_suggestions()
        return movement

    def transfer(self, body: dict) -> dict:
        part_id, src, dst, qty = body["part_id"], body["from_location_id"], body["to_location_id"], int(body["quantity"])
        if part_id not in self.parts or dst not in self.locations:
            raise HttpError(404, "pieza o ubicación desconocida")
        if self.on_hand[(part_id, src)] < qty:
            raise HttpError(409, "no alcanza el saldo en el origen")
        self.move("transfer_out", part_id, src, -qty, f"a {dst}")
        movement = self.move("transfer_in", part_id, dst, qty, f"de {src}")
        self.emit("stock.transferred", f"part:{part_id}", {
            "part_id": part_id, "from_location_id": src, "to_location_id": dst, "quantity": qty})
        return movement

    def count(self, body: dict) -> dict:
        if not (body.get("reason") or "").strip():
            raise HttpError(422, "el motivo es obligatorio")
        loc = body["location_id"]
        adjustments = []
        for line in body["lines"]:
            delta = int(line["counted_quantity"]) - self.on_hand[(line["part_id"], loc)]
            if delta:
                self.move("adjustment", line["part_id"], loc, delta, body["reason"])
                adjustments.append({"part_id": line["part_id"], "delta": delta})
                self.emit("stock.adjusted", f"part:{line['part_id']}", {
                    "part_id": line["part_id"], "location_id": loc, "delta": delta, "reason": body["reason"]})
        return {"adjustments": adjustments}

    def resolve(self, line_id: int, body: dict) -> dict:
        receipt = self.unmatched.get(line_id)
        if receipt is None or body.get("part_id") not in self.parts:
            raise HttpError(404, "recepción o pieza desconocida")
        if receipt["resolved"]:
            raise HttpError(409, "ya estaba resuelta")
        receipt["resolved"] = True
        return self.stock_in(body["part_id"], body.get("location_id") or RECEIVING, receipt)

    # --- consultas ---
    def availability(self, part_id: int) -> dict:
        if part_id not in self.parts:
            raise HttpError(404, "pieza desconocida")
        locs = [{"location_id": loc, "code": self.locations.get(loc, {}).get("code"), "on_hand": q, "reserved": 0, "available": q}
                for (p, loc), q in sorted(self.on_hand.items()) if p == part_id and q]
        on_hand, reserved = self.part_on_hand(part_id), self.part_reserved(part_id)
        return {"part_id": part_id, "on_hand": on_hand, "reserved": reserved, "available": on_hand - reserved, "locations": locs}

    def ledger(self, part_id: int) -> dict:
        if part_id not in self.parts:
            raise HttpError(404, "pieza desconocida")
        balance, entries = 0, []
        for m in self.movements:
            if m["part_id"] == part_id:
                balance += m["quantity"]
                entries.append({**m, "balance": balance})
        return {"part_id": part_id, "entries": entries[::-1]}

    def materials(self, code: str) -> dict:
        wo_id = self.order_by_code.get(code)
        if wo_id is None:
            raise HttpError(404, "orden desconocida")
        lines = [{"inspection_item_id": n.inspection_item_id, "bom_line_id": n.bom_line_id, "part_id": n.part_id,
                  "name": n.name, "required": n.required, "reserved": n.reserved, "issued": n.issued, "missing": n.missing}
                 for n in sorted(self.needs.values(), key=lambda n: n.seq) if n.wo_id == wo_id]
        return {"work_order_id": wo_id, "code": code, "lines": lines}

    def shortages(self, query: dict) -> dict:
        items = []
        for n in sorted(self.needs.values(), key=lambda n: n.seq):
            if n.missing == 0:
                continue
            code = self.code(n.wo_id)
            if query.get("work_order_code") and query["work_order_code"] != code:
                continue
            if query.get("part_id") and str(n.part_id) != query["part_id"]:
                continue
            items.append({"work_order_id": n.wo_id, "work_order_code": code, "part_id": n.part_id, "name": n.name,
                          "missing_quantity": n.missing, "inspection_item_id": n.inspection_item_id, "opened_at": n.opened_at})
        return {"items": items}

    def suggestions(self) -> dict:
        return {"items": [{"part_id": pid, "sku": self.parts[pid]["sku"], "name": self.parts[pid]["name"],
                           "suggested_quantity": qty, "work_order_ids": list(wos)}
                          for pid, (qty, wos) in sorted(self.last_suggestion.items())]}

    def unmatched_list(self) -> dict:
        return {"items": [{"id": lid, "purchase_line_id": lid, "part_number": r["part_number"], "description": r["description"],
                           "quantity": r["quantity"], "unit_price": r["unit_price"], "currency": r["currency"],
                           "received_at": r["received_at"], "work_order_code": r["work_order_code"]}
                          for lid, r in self.unmatched.items() if not r["resolved"]]}


def make_handler(inv: Inventory):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, status: int, payload) -> None:
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def route(self, method: str):
            url = urlparse(self.path)
            parts = url.path.strip("/").split("/")
            query = {k: v[0] for k, v in parse_qs(url.query).items()}
            body = {}
            if method == "POST":
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
            try:
                with inv.lock:
                    if method == "GET" and len(parts) == 3 and parts[0] == "parts" and parts[2] == "availability":
                        return self.reply(200, inv.availability(int(parts[1])))
                    if method == "GET" and len(parts) == 3 and parts[0] == "parts" and parts[2] == "ledger":
                        return self.reply(200, inv.ledger(int(parts[1])))
                    if method == "GET" and len(parts) == 3 and parts[0] == "work-orders" and parts[2] == "materials":
                        return self.reply(200, inv.materials(parts[1]))
                    if method == "GET" and parts == ["shortages"]:
                        return self.reply(200, inv.shortages(query))
                    if method == "GET" and parts == ["reorder-suggestions"]:
                        return self.reply(200, inv.suggestions())
                    if method == "GET" and parts == ["unmatched-receipts"]:
                        return self.reply(200, inv.unmatched_list())
                    if method == "POST" and len(parts) == 3 and parts[0] == "unmatched-receipts" and parts[2] == "resolve":
                        return self.reply(200, inv.resolve(int(parts[1]), body))
                    if method == "POST" and parts == ["issues"]:
                        return self.reply(201, inv.issue(body))
                    if method == "POST" and parts == ["transfers"]:
                        return self.reply(201, inv.transfer(body))
                    if method == "POST" and parts == ["counts"]:
                        return self.reply(201, inv.count(body))
                    self.reply(404, {"detail": "ruta desconocida"})
            except HttpError as err:
                self.reply(err.status, {"detail": err.detail})
            except (KeyError, ValueError, TypeError) as err:
                self.reply(422, {"detail": f"cuerpo inválido: {err}"})
            finally:
                inv.producer.flush(5)

        def do_GET(self):
            self.route("GET")

        def do_POST(self):
            self.route("POST")

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default="localhost:19092")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.broker, "enable.idempotence": True, "acks": "all"})
    inv = Inventory(producer)
    server = ThreadingHTTPServer(("0.0.0.0", args.port), make_handler(inv))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"referencia: API en :{args.port}", flush=True)

    # Primero el catálogo completo, después todo lo demás: si no, al releer desde cero
    # las compras llegan antes que sus piezas y se van a revisión manual.
    bootstrap = Consumer({"bootstrap.servers": args.broker, "group.id": f"reference-catalog-{uuid.uuid4()}",
                          "enable.auto.commit": False})
    meta = bootstrap.list_topics("shop.catalog", timeout=10).topics["shop.catalog"]
    ends = {}
    for p in meta.partitions:
        _, high = bootstrap.get_watermark_offsets(TopicPartition("shop.catalog", p), timeout=10)
        if high > 0:
            ends[p] = high
    bootstrap.assign([TopicPartition("shop.catalog", p, OFFSET_BEGINNING) for p in ends])
    while ends:
        msg = bootstrap.poll(1.0)
        if msg is None or msg.error():
            continue
        inv.handle(json.loads(msg.value()))
        if msg.offset() + 1 >= ends.get(msg.partition(), 0):
            ends.pop(msg.partition(), None)
    bootstrap.close()
    print(f"catálogo cargado: {len(inv.parts)} piezas", flush=True)

    consumer = Consumer({"bootstrap.servers": args.broker, "group.id": f"reference-{uuid.uuid4()}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe(INPUT_TOPICS)
    while True:
        msg = consumer.poll(0.5)
        if msg is None or msg.error():
            continue
        inv.handle(json.loads(msg.value()))
        producer.poll(0)
        producer.flush(5)


if __name__ == "__main__":
    main()
