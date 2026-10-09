"""Genera data/events.jsonl: el catálogo real + 6 semanas de operación sintética.

Entrada: organizer/catalog.raw.json (ver export_catalog.sql). Determinista por SEED.

    python3 organizer/generate_events.py

Qué es real y qué es sintético (detalle en data/README.md):
- Real: piezas, ubicaciones del taller, listas de partes de 5 modelos y la proporción
  bien/mal/falta + comprar/reparar de las evaluaciones capturadas en el taller.
- Sintético: órdenes de trabajo, inspecciones, rechazos/anulaciones, compras, precios,
  los cambios de catálogo a media operación, duplicados y desorden de entrega.
"""

from __future__ import annotations

import copy
import json
import random
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "organizer" / "catalog.raw.json"
OUT = ROOT / "data" / "events.jsonl"
SUMMARY = ROOT / "organizer" / "generation_summary.json"

SEED = 20261106
START = datetime(2026, 8, 3, 14, 0, tzinfo=timezone.utc)  # lunes 8:00 hora del taller
WEEKS = 6
END = START + timedelta(weeks=WEEKS)
WORK_ORDERS = 36

MODEL_WEIGHTS = {4: 30, 5: 25, 7: 25, 46: 10, 67: 10}  # el modelo 30 es de prueba, fuera
SKU_PLACEHOLDERS = {"CLAVE PENDIENTE", "????", "EQUIVALENCIAS", "OT XXXX"}
EXCLUDED_LOCATIONS = {9}  # "PRB": ubicación de prueba
SUPERVISORS = [12, 15, 21]

RECEIVING_LOCATION = {"location_id": 100, "code": "U-100", "name": "Recepción de material", "is_workbench": False}
STOCKROOM_BINS = [
    {"location_id": 100 + i, "code": f"U-{100 + i:03d}", "name": f"Almacén refacciones estante {shelf} nivel {level}", "is_workbench": False}
    for i, (shelf, level) in enumerate(((s, l) for s in "ABC" for l in range(1, 5)), start=1)
]

DAMAGE_NOTES = ["pista con picaduras", "desgaste visible", "reseco", "roto", "deformado", "con fuga", "oxidado", None, None, None]
REJECT_REASONS = [
    "Faltan fotos de las piezas dañadas",
    "Revisar cantidades del kit de sellos",
    "La pieza marcada como faltante sí venía en el equipo",
    "Volver a medir y evaluar rodamientos",
]
VOID_REASONS = [
    "Se regresó la etapa: el cliente envió el accesorio que faltaba",
    "Se regresó la etapa: diagnóstico hecho sobre el equipo equivocado",
]
CUSTOMER_ORDER_DESCRIPTIONS = [
    ("6205-2RS", "Rodamiento rígido de bolas 6205-2RS"),
    ("BES M12MI-PSC40B", "Sensor inductivo M12"),
    ("ACEITE ISO VG 32", "Aceite hidráulico ISO VG 32, cubeta 19 L"),
]


def utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class Event:
    topic: str
    event_type: str
    occurred_at: datetime
    key: str
    data: dict
    event_id: str = ""
    delay: timedelta | None = None  # retraso de entrega forzado (desorden deliberado)

    def envelope(self) -> dict:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_version": 1,
            "occurred_at": utc(self.occurred_at),
            "source": "shop-floor",
            "key": self.key,
            "data": self.data,
        }


@dataclass
class Sim:
    rng: random.Random
    raw: dict
    events: list[Event] = field(default_factory=list)
    parts: dict[int, dict] = field(default_factory=dict)
    boms: dict[int, dict] = field(default_factory=dict)  # model_id -> bom data (estado vigente)
    bom_history: list[tuple[datetime, int, dict]] = field(default_factory=list)
    stats: Counter = field(default_factory=Counter)
    next_ids: dict[str, int] = field(default_factory=lambda: {
        "work_order": 1201, "inspection": 401, "inspection_item": 9001,
        "purchase_request": 80, "request_number": 2041, "purchase_line": 520,
    })

    def new_id(self, kind: str) -> int:
        value = self.next_ids[kind]
        self.next_ids[kind] += 1
        return value

    def emit(self, topic: str, event_type: str, at: datetime, key: str, data: dict, delay: timedelta | None = None) -> None:
        self.events.append(Event(topic, event_type, at, key, copy.deepcopy(data), delay=delay))
        self.stats[event_type] += 1

    def bom_at(self, model_id: int, at: datetime) -> dict:
        current = None
        for when, mid, bom in self.bom_history:
            if mid == model_id and when <= at:
                current = bom
        return current


# --- catálogo ------------------------------------------------------------------
def build_catalog(sim: Sim) -> None:
    units = {u["id"]: u for u in sim.raw["units"]}
    t0 = START - timedelta(days=3)

    for p in sim.raw["pieces"]:
        code = (p["code"] or "").strip()
        family = p["family"]
        if family == "SERVICO":  # errata del catálogo real
            family = "SERVICIO"
        part = {
            "part_id": p["id"],
            "sku": None if (not code or code in SKU_PLACEHOLDERS) else code,
            "name": p["name"].strip(),
            "family": family,
            "group": p["group"],
            "subgroup": p["subgroup"],
            "unit": {"id": p["unit_id"], "name": units[p["unit_id"]]["name"]} if p["unit_id"] else None,
            "image_url": None,
            "active": True,
        }
        sim.parts[part["part_id"]] = part
        sim.emit("shop.catalog", "part.upserted", t0, f"part:{part['part_id']}", part)

    for loc in sim.raw["locations"]:
        if loc["id"] in EXCLUDED_LOCATIONS:
            continue
        data = {
            "location_id": loc["id"],
            "code": f"U-{loc['id']:03d}",
            "name": loc["name"].strip(),
            "is_workbench": loc["is_workstation"],
            "active": True,
        }
        sim.emit("shop.catalog", "location.upserted", t0, f"location:{loc['id']}", data)
    for data in [RECEIVING_LOCATION, *STOCKROOM_BINS]:
        sim.emit("shop.catalog", "location.upserted", t0, f"location:{data['location_id']}", {**data, "active": True})

    for m in sim.raw["models"]:
        if m["id"] not in MODEL_WEIGHTS:
            continue
        bom = {
            "model_id": m["id"],
            "model_name": m["name"],
            "brand": m["brand"],
            "lines": [
                {
                    "bom_line_id": ln["id"],
                    "part_id": ln["piece_id"],
                    "name": ln["name"].strip(),
                    "group_name": ln["group_name"],
                    "unit": {"id": ln["unit_id"], "name": units[ln["unit_id"]]["name"]} if ln["unit_id"] else None,
                    "qty_per_unit": ln["qty"],
                    "sort_order": ln["sort_order"],
                }
                for ln in m["lines"]
            ],
        }
        sim.boms[m["id"]] = bom
        sim.bom_history.append((t0, m["id"], copy.deepcopy(bom)))
        sim.emit("shop.catalog", "bom.upserted", t0, f"model:{m['id']}", bom)


def catalog_changes(sim: Sim) -> None:
    """Cambios de catálogo a media operación: sku capturado tarde, pieza dada de baja,
    grupo renombrado y una pieza NUEVA que llega por el tópico DESPUÉS de la inspección
    que ya la usa."""
    rng = sim.rng
    # 1) Una pieza sin sku recibe su clave (las compras previas no pudieron relacionarse).
    no_sku = sorted(pid for pid, p in sim.parts.items() if p["sku"] is None and p["family"] == "REFACCIÓN")
    pid = no_sku[0]
    sim.parts[pid]["sku"] = f"FLECHA-{900 + pid}"
    sim.emit("shop.catalog", "part.upserted", START + timedelta(days=9, hours=3), f"part:{pid}", sim.parts[pid])
    sim.stats["catalog:sku_filled_late"] = pid

    # 2) Una pieza fuera de toda lista de partes se da de baja.
    in_bom = {ln["part_id"] for b in sim.boms.values() for ln in b["lines"] if ln["part_id"]}
    candidates = sorted(p for p in sim.parts if p not in in_bom)
    pid = rng.choice(candidates)
    sim.parts[pid]["active"] = False
    sim.emit("shop.catalog", "part.upserted", START + timedelta(days=16, hours=5), f"part:{pid}", sim.parts[pid])
    sim.stats["catalog:deactivated"] = pid

    # 3) Pieza nueva + renombre de grupo en el modelo 4 (mismo bom.upserted).
    changed_at = START + timedelta(days=18, hours=2)
    new_part = {
        "part_id": max(sim.parts) + 1,
        "sku": "7014 CTA/P4",
        "name": "RODAMIENTO DE CONTACTO ANGULAR 7014",
        "family": "REFACCIÓN",
        "group": "RODAMIENTOS",
        "subgroup": None,
        "unit": {"id": 1, "name": "PIEZA"},
        "image_url": None,
        "active": True,
    }
    sim.parts[new_part["part_id"]] = new_part
    # Llega con un día de retraso: las inspecciones del modelo 4 la referencian antes.
    sim.emit("shop.catalog", "part.upserted", changed_at, f"part:{new_part['part_id']}", new_part, delay=timedelta(days=1))
    sim.stats["catalog:late_new_part"] = new_part["part_id"]

    bom = copy.deepcopy(sim.boms[4])
    renamed = None
    for ln in bom["lines"]:
        if ln["group_name"]:
            renamed = renamed or ln["group_name"]
            if ln["group_name"] == renamed:
                ln["group_name"] = f"{renamed} FRONTAL"
    bom["lines"].append({
        "bom_line_id": 900,
        "part_id": new_part["part_id"],
        "name": new_part["name"],
        "group_name": None,
        "unit": {"id": 1, "name": "PIEZA"},
        "qty_per_unit": 2,
        "sort_order": max(ln["sort_order"] for ln in bom["lines"]) + 1,
    })
    sim.boms[4] = bom
    sim.bom_history.append((changed_at, 4, copy.deepcopy(bom)))
    sim.emit("shop.catalog", "bom.upserted", changed_at, "model:4", bom)
    sim.stats["catalog:renamed_group"] = renamed


# --- inspecciones --------------------------------------------------------------
def evaluate_lines(sim: Sim, bom: dict, previous: list[dict] | None = None) -> list[dict]:
    """Un renglón por línea de la lista de partes. Las líneas de un mismo kit se evalúan
    casi siempre igual. Con `previous` (rehacer tras rechazo/anulación) copia la
    evaluación anterior y cambia ~20% de los renglones."""
    rng = sim.rng
    evaluations = sim.raw["evaluations"]
    prev_by_line = {it["bom_line_id"]: it for it in previous or []}

    def sample(line: dict) -> tuple[str, str | None, int | None]:
        ev = rng.choice(evaluations)
        condition = {"bien": "ok", "mal": "damaged", "falta": "missing"}[ev["status"]]
        action = {"comprar": "buy", "reparar": "repair", None: None}[ev["action"]]
        if condition == "ok":
            return condition, None, None
        cap = line["qty_per_unit"]
        if cap is None:
            qty = None if rng.random() < 0.4 else rng.choice([1, 1, 2])
        else:
            qty = max(1, min(cap, ev["qty"] or 1))
        return condition, action, qty

    groups: dict[str, tuple] = {}
    items = []
    for line in bom["lines"]:
        prev = prev_by_line.get(line["bom_line_id"])
        if prev is not None and rng.random() > 0.2:
            condition, action, qty = prev["condition"], prev["action"], prev["quantity"]
        else:
            group_key = line["group_name"] or line["name"]
            if group_key in groups and rng.random() < 0.7:
                condition, action, _ = groups[group_key]
                qty = None if condition == "ok" else (line["qty_per_unit"] or groups[group_key][2])
            else:
                condition, action, qty = sample(line)
                groups[group_key] = (condition, action, qty)
        items.append({
            "inspection_item_id": sim.new_id("inspection_item"),
            "bom_line_id": line["bom_line_id"],
            "part_id": line["part_id"],
            "name": line["name"],
            "group_name": line["group_name"],
            "condition": condition,
            "action": action,
            "quantity": qty,
            "note": rng.choice(DAMAGE_NOTES) if condition == "damaged" else None,
        })
    return items


def full_inspection_findings(sim: Sim, bom: dict, quick_items: list[dict]) -> list[dict]:
    """Hallazgos del diagnóstico completo: casi siempre nada (items: []). A veces 1-3
    renglones: una línea nueva o la misma línea del rápido con OTRA cantidad (la
    necesidad se reemplaza, no se suma)."""
    rng = sim.rng
    if rng.random() < 0.7:
        return []
    lines = rng.sample(bom["lines"], k=min(len(bom["lines"]), rng.randint(1, 3)))
    quick_by_line = {it["bom_line_id"]: it for it in quick_items}
    items = []
    for line in lines:
        prev = quick_by_line.get(line["bom_line_id"])
        base = prev["quantity"] if prev and prev["quantity"] else 1
        qty = base + rng.randint(1, 2)
        if line["qty_per_unit"]:
            qty = min(qty, line["qty_per_unit"])
        items.append({
            "inspection_item_id": sim.new_id("inspection_item"),
            "bom_line_id": line["bom_line_id"],
            "part_id": line["part_id"],
            "name": line["name"],
            "group_name": line["group_name"],
            "condition": "damaged",
            "action": "buy",
            "quantity": qty,
            "note": "hallazgo en diagnóstico completo",
        })
    return items


@dataclass
class WorkOrder:
    work_order_id: int
    code: str
    model_id: int
    received_at: datetime
    stage: str = "awaiting_quick_inspection"
    # Necesidad vigente por línea de lista de partes: bom_line_id -> renglón aprobado.
    requirement: dict[int, dict] = field(default_factory=dict)
    deleted: bool = False


def change_stage(sim: Sim, wo: WorkOrder, to_stage: str, at: datetime) -> None:
    sim.emit("shop.work_orders", "work_order.stage_changed", at, wo.code,
             {"work_order_id": wo.work_order_id, "from_stage": wo.stage, "to_stage": to_stage})
    wo.stage = to_stage


def run_inspection(sim: Sim, wo: WorkOrder, kind: str, items: list[dict], at: datetime) -> tuple[int, datetime]:
    inspection_id = sim.new_id("inspection")
    base = {"inspection_id": inspection_id, "work_order_id": wo.work_order_id, "kind": kind, "items": items}
    sim.emit("shop.inspections", "inspection.submitted", at, wo.code, base)
    return inspection_id, at


def approve(sim: Sim, wo: WorkOrder, inspection_id: int, kind: str, items: list[dict], at: datetime) -> None:
    data = {"inspection_id": inspection_id, "work_order_id": wo.work_order_id, "kind": kind, "items": items,
            "approved_by": sim.rng.choice(SUPERVISORS), "approved_at": utc(at)}
    sim.emit("shop.inspections", "inspection.approved", at, wo.code, data)
    for it in items:
        if it["action"] == "buy":
            wo.requirement[it["bom_line_id"]] = it


def hours(rng: random.Random, lo: float, hi: float) -> timedelta:
    return timedelta(hours=rng.uniform(lo, hi))


def simulate_work_order(sim: Sim, wo: WorkOrder, fate: str, purchases: list) -> None:
    rng = sim.rng
    t = wo.received_at
    sim.emit("shop.work_orders", "work_order.opened", t, wo.code, {
        "work_order_id": wo.work_order_id, "code": wo.code, "model_id": wo.model_id,
        "department": "spindles", "received_at": utc(t),
    })

    t += hours(rng, 2, 30)
    change_stage(sim, wo, "quick_inspection", t)

    items = evaluate_lines(sim, sim.bom_at(wo.model_id, t))
    t += hours(rng, 4, 24)
    inspection_id, _ = run_inspection(sim, wo, "quick", items, t)

    if fate == "discarded":
        t += hours(rng, 1, 6)
        sim.emit("shop.inspections", "inspection.discarded", t, wo.code,
                 {"inspection_id": inspection_id, "work_order_id": wo.work_order_id})
        change_stage(sim, wo, "awaiting_quick_inspection", t)
        t += hours(rng, 4, 30)
        change_stage(sim, wo, "quick_inspection", t)
        items = evaluate_lines(sim, sim.bom_at(wo.model_id, t), previous=None)
        t += hours(rng, 4, 24)
        inspection_id, _ = run_inspection(sim, wo, "quick", items, t)

    if fate == "rejected":
        t += hours(rng, 1, 8)
        sim.emit("shop.inspections", "inspection.rejected", t, wo.code,
                 {"inspection_id": inspection_id, "work_order_id": wo.work_order_id, "reason": rng.choice(REJECT_REASONS)})
        items = evaluate_lines(sim, sim.bom_at(wo.model_id, t), previous=items)
        t += hours(rng, 4, 24)
        inspection_id, _ = run_inspection(sim, wo, "quick", items, t)

    t += hours(rng, 1, 10)
    approve(sim, wo, inspection_id, "quick", items, t)
    approved_quick = items
    t += timedelta(minutes=1)
    change_stage(sim, wo, "awaiting_full_inspection", t)
    queue_purchases(sim, wo, approved_quick, t, purchases)

    if fate == "voided":
        t += hours(rng, 6, 48)
        change_stage(sim, wo, "quick_inspection", t)
        sim.emit("shop.inspections", "inspection.voided", t, wo.code,
                 {"inspection_id": inspection_id, "work_order_id": wo.work_order_id, "reason": rng.choice(VOID_REASONS)})
        wo.requirement.clear()
        items = evaluate_lines(sim, sim.bom_at(wo.model_id, t), previous=approved_quick)
        t += hours(rng, 4, 24)
        inspection_id, _ = run_inspection(sim, wo, "quick", items, t)
        t += hours(rng, 1, 10)
        approve(sim, wo, inspection_id, "quick", items, t)
        approved_quick = items
        t += timedelta(minutes=1)
        change_stage(sim, wo, "awaiting_full_inspection", t)

    if fate == "deleted":
        t += hours(rng, 24, 96)
        sim.emit("shop.work_orders", "work_order.deleted", t, wo.code, {"work_order_id": wo.work_order_id, "code": wo.code})
        wo.deleted = True
        return

    # Diagnóstico completo: 0-3 procesos, cada uno una inspección `full`.
    processes = rng.choice([0, 1, 1, 2, 3])
    if processes:
        t += hours(rng, 24, 72)
        change_stage(sim, wo, "full_inspection", t)
        for _ in range(processes):
            findings = full_inspection_findings(sim, sim.bom_at(wo.model_id, t), approved_quick)
            t += hours(rng, 6, 30)
            inspection_id, _ = run_inspection(sim, wo, "full", findings, t)
            t += hours(rng, 1, 8)
            approve(sim, wo, inspection_id, "full", findings, t)
            queue_purchases(sim, wo, findings, t, purchases)

    for stage in ["cleaning", "assembly", "run_in", "final_assembly", "final_release"]:
        t += hours(rng, 24, 96)
        if t >= END:
            break
        change_stage(sim, wo, stage, t)


# --- compras -------------------------------------------------------------------
def price_for(part: dict | None, rng: random.Random) -> tuple[str, str]:
    group = (part or {}).get("group") or ""
    if group == "RODAMIENTOS":
        return ("USD", f"{rng.uniform(150, 900):.2f}") if rng.random() < 0.7 else ("MXN", f"{rng.uniform(2500, 18000):.2f}")
    if group in ("SISTEMA DE RETROALIMENTACION",):
        return ("USD", f"{rng.uniform(40, 650):.2f}") if rng.random() < 0.7 else ("MXN", f"{rng.uniform(700, 12000):.2f}")
    if group == "SISTEMA DE SELLADO":
        return "MXN", f"{rng.uniform(15, 140):.2f}"
    if group in ("FLECHA", "ESTATOR"):
        return "MXN", f"{rng.uniform(15000, 80000):.2f}"
    return "MXN", f"{rng.uniform(40, 3500):.2f}"


def part_number_for(part: dict | None, fallback_name: str, rng: random.Random) -> str:
    """Cómo escribe Compras el número de parte: igual al sku, con variaciones de
    formato, o con el número del proveedor (no relacionable)."""
    sku = part["sku"] if part else None
    if not sku:
        return f"S/N {fallback_name}"
    r = rng.random()
    if r < 0.55:
        return sku
    if r < 0.65:
        return sku.lower()
    if r < 0.75:
        return "  " + sku.replace("-", " - ") + " "
    if r < 0.85:
        return sku.replace("-", "").replace(".", "").replace(" ", "")
    if r < 0.92:
        return f"NP: {sku}"
    return f"PROV-{rng.randint(10000, 99999)}"


def queue_purchases(sim: Sim, wo: WorkOrder, items: list[dict], approved_at: datetime, purchases: list) -> None:
    """Compras reacciona a lo aprobado (sin saber si ya había en almacén — justo el
    problema del reto). ~70% de los renglones a comprar se compran."""
    rng = sim.rng
    lines = []
    for it in items:
        if it["action"] != "buy" or rng.random() > 0.7:
            continue
        qty = it["quantity"] or 1
        part = sim.parts.get(it["part_id"]) if it["part_id"] else None
        if part and part["group"] == "SISTEMA DE SELLADO" and rng.random() < 0.5:
            qty = max(qty, 5)  # se compran por paquete
        lines.append((it, part, qty))
    if not lines:
        return
    received_at = approved_at + timedelta(days=rng.uniform(3, 15))
    purchases.append({
        "received_at": received_at,
        "work_order_code": wo.code if rng.random() < 0.7 else None,
        "purpose": "restock",
        "lines": [(part, it["name"], qty) for it, part, qty in lines],
    })


def opening_stock(sim: Sim) -> dict:
    """Inventario inicial: entra como una compra de arranque antes de la operación."""
    rng = sim.rng
    in_bom = sorted({ln["part_id"] for b in sim.boms.values() for ln in b["lines"] if ln["part_id"]})
    others = sorted(p for p in sim.parts if p not in in_bom and sim.parts[p]["active"])
    chosen = [p for p in in_bom if rng.random() < 0.55] + [p for p in others if rng.random() < 0.2]
    lines = []
    for pid in chosen:
        part = sim.parts[pid]
        qty = rng.randint(5, 30) if part["group"] == "SISTEMA DE SELLADO" else rng.randint(1, 6)
        lines.append((part, part["name"], qty))
    return {"received_at": START - timedelta(days=2), "work_order_code": None, "purpose": "restock", "lines": lines}


def periodic_purchases(sim: Sim) -> list[dict]:
    """Reabasto semanal de consumibles y una compra semanal para un cliente (no es
    existencia del taller)."""
    rng = sim.rng
    consumables = sorted(p for p, part in sim.parts.items()
                         if part["group"] in ("SISTEMA DE SELLADO", "OTROS") and part["active"])
    out = []
    for week in range(WEEKS):
        monday = START + timedelta(weeks=week)
        picks = rng.sample(consumables, k=rng.randint(4, 9))
        out.append({
            "received_at": monday + timedelta(days=rng.uniform(1, 4)),
            "work_order_code": None,
            "purpose": "restock",
            "lines": [(sim.parts[p], sim.parts[p]["name"], rng.randint(5, 20)) for p in picks],
        })
        cust_lines = []
        for _ in range(rng.randint(2, 3)):
            if rng.random() < 0.5:  # una pieza que SÍ está en el catálogo: trampa
                part = sim.parts[rng.choice(consumables)]
                cust_lines.append((part, part["name"], rng.randint(2, 10)))
            else:
                pn, desc = rng.choice(CUSTOMER_ORDER_DESCRIPTIONS)
                cust_lines.append(({"sku": pn, "group": None, "name": desc}, desc, rng.randint(1, 4)))
        out.append({
            "received_at": monday + timedelta(days=rng.uniform(1, 4)),
            "work_order_code": None,
            "purpose": "customer_order",
            "lines": cust_lines,
        })
    return out


def emit_purchases(sim: Sim, purchases: list[dict]) -> None:
    rng = sim.rng
    for pr in sorted(purchases, key=lambda p: p["received_at"]):
        if pr["received_at"] >= END:
            continue
        request_id = sim.new_id("purchase_request")
        number = str(sim.new_id("request_number"))
        at = pr["received_at"]
        for part, fallback_name, qty in pr["lines"]:
            at += timedelta(minutes=rng.randint(1, 12))  # Compras recibe línea por línea
            currency, price = price_for(part if part and "part_id" in part else None, rng)
            exact = pr["purpose"] == "customer_order" and part and "part_id" not in part
            data = {
                "purchase_request_id": request_id,
                "request_number": number,
                "line_id": sim.new_id("purchase_line"),
                "part_number": part["sku"] if exact else part_number_for(part, fallback_name, rng),
                "description": (part or {}).get("name") or fallback_name,
                "quantity": qty,
                "unit_price": price,
                "currency": currency,
                "purpose": pr["purpose"],
                "received_at": utc(at),
                "work_order_code": pr["work_order_code"],
            }
            sim.emit("shop.purchasing", "purchase.item_received", at, f"PR-{number}", data)


# --- entrega: desorden + duplicados --------------------------------------------
def plant_late_deliveries(sim: Sim) -> None:
    """Desorden entre tópicos garantizado (no al azar): la pieza nueva llega DESPUÉS de
    la primera inspección que la usa, y 3 órdenes llegan después de su primera
    inspección."""
    new_part_id = sim.stats["catalog:late_new_part"]
    uses = [e.occurred_at for e in sim.events
            if e.event_type.startswith("inspection.") and any(it["part_id"] == new_part_id for it in e.data.get("items", []))]
    for e in sim.events:
        if e.event_type == "part.upserted" and e.data["part_id"] == new_part_id and uses:
            e.delay = min(uses) - e.occurred_at + timedelta(hours=3)

    first_inspection: dict[str, datetime] = {}
    for e in sim.events:
        if e.event_type == "inspection.submitted":
            first_inspection.setdefault(e.key, e.occurred_at)
    opened = [e for e in sim.events if e.event_type == "work_order.opened" and e.key in first_inspection]
    for e in sim.rng.sample(opened, 3):
        e.delay = first_inspection[e.key] - e.occurred_at + timedelta(hours=2)
        sim.stats.setdefault("delivery:work_orders_late", []).append(e.key)



def deliver(sim: Sim) -> list[dict]:
    rng = sim.rng
    for ev in sim.events:
        ev.event_id = str(uuid.UUID(int=rng.getrandbits(128), version=4))

    scheduled = []
    for ev in sorted(sim.events, key=lambda e: e.occurred_at):
        if ev.delay is not None:
            jitter = ev.delay
        elif rng.random() < 0.05:
            jitter = timedelta(hours=rng.uniform(1, 6))  # llega tarde
        else:
            jitter = timedelta(seconds=rng.uniform(0, 1200))
        scheduled.append([ev.occurred_at + jitter, ev])

    # Orden garantizado por (tópico, key): la entrega nunca rebasa al evento anterior.
    last: dict[tuple[str, str], datetime] = {}
    for item in scheduled:
        when, ev = item
        k = (ev.topic, ev.key)
        if k in last and when <= last[k]:
            when = last[k] + timedelta(seconds=1)
        item[0] = when
        last[k] = when

    deliveries = [(when, ev) for when, ev in scheduled]
    duplicated = 0
    for when, ev in list(deliveries):
        if rng.random() < 0.04:  # al menos una vez: reentrega del mismo event_id
            deliveries.append((when + timedelta(minutes=rng.uniform(1, 720)), ev))
            duplicated += 1
    sim.stats["delivery:duplicates"] = duplicated
    deliveries.sort(key=lambda d: d[0])

    out_of_order = sum(1 for (a, ea), (b, eb) in zip(deliveries, deliveries[1:]) if eb.occurred_at < ea.occurred_at)
    sim.stats["delivery:out_of_order_pairs"] = out_of_order
    return [{"topic": ev.topic, "replay_at": utc(when), "event": ev.envelope()} for when, ev in deliveries]


# --- main ----------------------------------------------------------------------
def work_order_code(received_at: datetime, per_day: Counter) -> str:
    local = received_at - timedelta(hours=6)
    day = local.strftime("%d%m%y")
    seq = per_day[day]
    per_day[day] += 1
    return f"R{day}{seq:03d}"


def main() -> None:
    rng = random.Random(SEED)
    sim = Sim(rng=rng, raw=json.loads(RAW.read_text(encoding="utf-8")))
    build_catalog(sim)
    catalog_changes(sim)

    fates = ["normal"] * WORK_ORDERS
    for i, fate in zip(rng.sample(range(WORK_ORDERS), 12),
                       ["rejected"] * 6 + ["discarded"] * 2 + ["voided"] * 2 + ["deleted"] * 2):
        fates[i] = fate

    per_day: Counter = Counter()
    models, weights = zip(*MODEL_WEIGHTS.items())
    arrivals = sorted(START + timedelta(days=rng.uniform(0, WEEKS * 7 - 9)) for _ in range(WORK_ORDERS))
    purchases = [opening_stock(sim), *periodic_purchases(sim)]
    for received_at, fate in zip(arrivals, fates):
        received_at = received_at.replace(hour=rng.randint(14, 23), minute=rng.randint(0, 59), second=0)
        wo = WorkOrder(sim.new_id("work_order"), work_order_code(received_at, per_day),
                       rng.choices(models, weights)[0], received_at)
        sim.stats[f"work_orders:{fate}"] += 1
        simulate_work_order(sim, wo, fate, purchases)

    emit_purchases(sim, purchases)
    sim.events = [e for e in sim.events if e.occurred_at < END]
    plant_late_deliveries(sim)
    lines = deliver(sim)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8") as fh:
        for line in lines:
            fh.write(json.dumps(line, ensure_ascii=False) + "\n")

    summary = {"seed": SEED, "start": utc(START), "end": utc(END), "lines": len(lines), **dict(sorted(sim.stats.items()))}
    SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
