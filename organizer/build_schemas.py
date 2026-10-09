"""Genera contract/schemas/** (JSON Schema 2020-12) desde una sola definición.

Fuente de verdad del contrato de eventos. Si cambias algo aquí, vuelve a correr:
    python3 organizer/build_schemas.py
y actualiza contract/README.md en el mismo cambio.
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "contract" / "schemas"
BASE_ID = "https://hackathon-inventario.local/schemas"

STAGES = [
    "awaiting_quick_inspection",
    "quick_inspection",
    "awaiting_full_inspection",
    "full_inspection",
    "cleaning",
    "assembly",
    "run_in",
    "final_assembly",
    "final_release",
]
DEPARTMENTS = ["spindles", "motors", "electronics", "torque_center"]
CURRENCIES = ["MXN", "USD"]


# --- tipos reutilizables -----------------------------------------------------
def ref(name: str) -> dict:
    return {"$ref": f"../common.schema.json#/$defs/{name}"}


def nullable(schema: dict) -> dict:
    return {"anyOf": [schema, {"type": "null"}]}


COMMON_DEFS = {
    "id": {"type": "integer", "minimum": 1, "description": "ID entero positivo."},
    "quantity": {
        "type": "integer",
        "minimum": 1,
        "description": "Cantidad entera, en la unidad de la pieza.",
    },
    "decimal": {
        "type": "string",
        "pattern": r"^-?\d+(\.\d+)?$",
        "description": "Importe decimal en texto, ej. \"1850.00\". Nunca float.",
    },
    "timestamp": {
        "type": "string",
        "format": "date-time",
        "pattern": r"Z$",
        "description": "ISO 8601 en UTC, terminado en Z.",
    },
    "currency": {"enum": CURRENCIES},
    "stage": {"enum": STAGES},
    "department": {"enum": DEPARTMENTS},
    "work_order_code": {
        "type": "string",
        "pattern": r"^R\d{9}(-\d+)?$",
        "description": "Código de la orden de trabajo: R + DDMMYY + consecutivo de 3 dígitos; las garantías agregan -N.",
    },
    "unit": nullable(
        {
            "type": "object",
            "additionalProperties": False,
            "required": ["id", "name"],
            "properties": {"id": ref("id"), "name": {"type": "string", "minLength": 1}},
        }
    ),
    "non_empty_string": {"type": "string", "minLength": 1},
    "inspection_item": {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "inspection_item_id",
            "bom_line_id",
            "part_id",
            "name",
            "group_name",
            "condition",
            "action",
            "quantity",
            "note",
        ],
        "properties": {
            "inspection_item_id": ref("id"),
            "bom_line_id": ref("id"),
            "part_id": nullable(ref("id")),
            "name": ref("non_empty_string"),
            "group_name": nullable(ref("non_empty_string")),
            "condition": {"enum": ["ok", "damaged", "missing"]},
            "action": {"enum": ["buy", "repair", None]},
            "quantity": nullable(ref("quantity")),
            "note": nullable({"type": "string"}),
        },
        # ok → sin acción; missing → buy; damaged → buy | repair.
        "allOf": [
            {
                "if": {"properties": {"condition": {"const": "ok"}}},
                "then": {"properties": {"action": {"const": None}, "quantity": {"const": None}}},
            },
            {
                "if": {"properties": {"condition": {"const": "missing"}}},
                "then": {"properties": {"action": {"const": "buy"}}},
            },
            {
                "if": {"properties": {"condition": {"const": "damaged"}}},
                "then": {"properties": {"action": {"enum": ["buy", "repair"]}}},
            },
        ],
    },
}


# --- eventos -----------------------------------------------------------------
def obj(required: dict, description: str | None = None) -> dict:
    """Objeto cerrado: todos los campos listados son obligatorios (pueden ser null)."""
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": list(required),
        "properties": required,
    }
    if description:
        schema["description"] = description
    return schema


INSPECTION_BASE = {
    "inspection_id": ref("id"),
    "work_order_id": ref("id"),
    "kind": {"enum": ["quick", "full"]},
    "items": {"type": "array", "items": ref("inspection_item")},
}

INPUT_EVENTS = {
    "part.upserted": {
        "topic": "shop.catalog",
        "key": "\"part:\" + part_id, ej. \"part:305\"",
        "data": obj(
            {
                "part_id": ref("id"),
                "sku": nullable(ref("non_empty_string")),
                "name": ref("non_empty_string"),
                "family": nullable({"type": "string"}),
                "group": nullable({"type": "string"}),
                "subgroup": nullable({"type": "string"}),
                "unit": ref("unit"),
                "image_url": nullable({"type": "string"}),
                "active": {"type": "boolean"},
            }
        ),
    },
    "location.upserted": {
        "topic": "shop.catalog",
        "key": "\"location:\" + location_id, ej. \"location:14\"",
        "data": obj(
            {
                "location_id": ref("id"),
                "code": {"type": "string", "pattern": r"^U-\d{3}$"},
                "name": ref("non_empty_string"),
                "is_workbench": {"type": "boolean"},
                "active": {"type": "boolean"},
            }
        ),
    },
    "bom.upserted": {
        "topic": "shop.catalog",
        "key": "\"model:\" + model_id, ej. \"model:4\"",
        "data": obj(
            {
                "model_id": ref("id"),
                "model_name": ref("non_empty_string"),
                "brand": ref("non_empty_string"),
                "lines": {
                    "type": "array",
                    "items": obj(
                        {
                            "bom_line_id": ref("id"),
                            "part_id": nullable(ref("id")),
                            "name": ref("non_empty_string"),
                            "group_name": nullable(ref("non_empty_string")),
                            "unit": ref("unit"),
                            "qty_per_unit": nullable(ref("quantity")),
                            "sort_order": {"type": "integer"},
                        }
                    ),
                },
            },
            "Lista de partes COMPLETA del modelo: reemplaza a la anterior.",
        ),
    },
    "work_order.opened": {
        "topic": "shop.work_orders",
        "key": "código de la orden",
        "data": obj(
            {
                "work_order_id": ref("id"),
                "code": ref("work_order_code"),
                "model_id": ref("id"),
                "department": ref("department"),
                "received_at": ref("timestamp"),
            }
        ),
    },
    "work_order.stage_changed": {
        "topic": "shop.work_orders",
        "key": "código de la orden",
        "data": obj(
            {
                "work_order_id": ref("id"),
                "from_stage": ref("stage"),
                "to_stage": ref("stage"),
            }
        ),
    },
    "work_order.deleted": {
        "topic": "shop.work_orders",
        "key": "código de la orden",
        "data": obj({"work_order_id": ref("id"), "code": ref("work_order_code")}),
    },
    "inspection.submitted": {
        "topic": "shop.inspections",
        "key": "código de la orden",
        "data": obj(dict(INSPECTION_BASE)),
    },
    "inspection.approved": {
        "topic": "shop.inspections",
        "key": "código de la orden",
        "data": obj(
            {**INSPECTION_BASE, "approved_by": ref("id"), "approved_at": ref("timestamp")}
        ),
    },
    "inspection.rejected": {
        "topic": "shop.inspections",
        "key": "código de la orden",
        "data": obj(
            {
                "inspection_id": ref("id"),
                "work_order_id": ref("id"),
                "reason": ref("non_empty_string"),
            }
        ),
    },
    "inspection.discarded": {
        "topic": "shop.inspections",
        "key": "código de la orden",
        "data": obj({"inspection_id": ref("id"), "work_order_id": ref("id")}),
    },
    "inspection.voided": {
        "topic": "shop.inspections",
        "key": "código de la orden",
        "data": obj(
            {
                "inspection_id": ref("id"),
                "work_order_id": ref("id"),
                "reason": ref("non_empty_string"),
            },
            "Una inspección YA APROBADA se anula (la orden regresó de etapa).",
        ),
    },
    "purchase.item_received": {
        "topic": "shop.purchasing",
        "key": "\"PR-\" + request_number, ej. \"PR-2044\"",
        "data": obj(
            {
                "purchase_request_id": ref("id"),
                "request_number": ref("non_empty_string"),
                "line_id": ref("id"),
                "part_number": {"type": "string"},
                "description": ref("non_empty_string"),
                "quantity": ref("quantity"),
                "unit_price": ref("decimal"),
                "currency": ref("currency"),
                "purpose": {"enum": ["restock", "customer_order"]},
                "received_at": ref("timestamp"),
                "work_order_code": nullable(ref("work_order_code")),
            }
        ),
    },
}

OUTPUT_EVENTS = {
    "stock.received": {
        "key": "\"part:\" + part_id",
        "data": obj(
            {
                "part_id": ref("id"),
                "location_id": ref("id"),
                "quantity": ref("quantity"),
                "unit_cost": ref("decimal"),
                "currency": ref("currency"),
                "purchase_line_id": ref("id"),
            }
        ),
    },
    "stock.unmatched_receipt": {
        "key": "\"purchase_line:\" + purchase_line_id",
        "data": obj(
            {
                "purchase_line_id": ref("id"),
                "part_number": {"type": "string"},
                "description": ref("non_empty_string"),
                "quantity": ref("quantity"),
            }
        ),
    },
    "stock.reserved": {
        "key": "código de la orden",
        "data": obj(
            {
                "reservation_id": {"type": ["integer", "string"]},
                "work_order_id": ref("id"),
                "part_id": ref("id"),
                "quantity": ref("quantity"),
                "inspection_item_id": ref("id"),
            }
        ),
    },
    "stock.shortage_detected": {
        "key": "código de la orden",
        "data": obj(
            {
                "work_order_id": ref("id"),
                "part_id": nullable(ref("id")),
                "name": ref("non_empty_string"),
                "missing_quantity": nullable(ref("quantity")),
                "inspection_item_id": ref("id"),
            }
        ),
    },
    "stock.shortage_resolved": {
        "key": "código de la orden",
        "data": obj(
            {
                "work_order_id": ref("id"),
                "part_id": ref("id"),
                "quantity": ref("quantity"),
            }
        ),
    },
    "stock.issued": {
        "key": "código de la orden",
        "data": obj(
            {
                "work_order_id": ref("id"),
                "part_id": ref("id"),
                "location_id": ref("id"),
                "quantity": ref("quantity"),
                "issued_by": {"type": ["integer", "string"], "minLength": 1},
            }
        ),
    },
    "stock.transferred": {
        "key": "\"part:\" + part_id",
        "data": obj(
            {
                "part_id": ref("id"),
                "from_location_id": ref("id"),
                "to_location_id": ref("id"),
                "quantity": ref("quantity"),
            }
        ),
    },
    "stock.adjusted": {
        "key": "\"part:\" + part_id",
        "data": obj(
            {
                "part_id": ref("id"),
                "location_id": ref("id"),
                "delta": {"type": "integer", "not": {"const": 0}},
                "reason": ref("non_empty_string"),
            }
        ),
    },
    "stock.reorder_suggested": {
        "key": "\"part:\" + part_id",
        "data": obj(
            {
                "part_id": ref("id"),
                "suggested_quantity": ref("quantity"),
                "work_order_ids": {"type": "array", "items": ref("id"), "uniqueItems": True},
            }
        ),
    },
}


def envelope(event_type: str, source: str, data: dict, key_desc: str, extra: dict) -> dict:
    direction = "input" if source == "shop-floor" else "output"
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"{BASE_ID}/{direction}/{event_type}.schema.json",
        "title": event_type,
        "description": f"Llave (key): {key_desc}." + (f" Tópico: {extra['topic']}." if "topic" in extra else ""),
        "type": "object",
        "additionalProperties": False,
        "required": ["event_id", "event_type", "event_version", "occurred_at", "source", "key", "data"],
        "properties": {
            "event_id": {"type": "string", "format": "uuid"},
            "event_type": {"const": event_type},
            "event_version": {"const": 1},
            "occurred_at": ref("timestamp"),
            "source": {"const": source},
            "key": ref("non_empty_string"),
            "data": data,
        },
    }


def write(path: Path, schema: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(schema, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    common = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"{BASE_ID}/common.schema.json",
        "title": "Tipos compartidos",
        # Dentro del propio archivo las referencias son locales.
        "$defs": json.loads(json.dumps(COMMON_DEFS).replace('"../common.schema.json#', '"#')),
    }
    write(OUT / "common.schema.json", common)

    index = {"input": {}, "output": {}}
    for name, spec in INPUT_EVENTS.items():
        write(OUT / "input" / f"{name}.schema.json", envelope(name, "shop-floor", spec["data"], spec["key"], spec))
        index["input"][name] = {"topic": spec["topic"], "schema": f"input/{name}.schema.json"}
    for name, spec in OUTPUT_EVENTS.items():
        # Salidas: los campos pedidos son obligatorios, pero pueden agregar más.
        spec["data"]["additionalProperties"] = True
        write(OUT / "output" / f"{name}.schema.json", envelope(name, "inventory", spec["data"], spec["key"], {}))
        index["output"][name] = {"topic": "inventory.events", "schema": f"output/{name}.schema.json"}
    write(OUT / "index.json", index)
    print(f"{len(INPUT_EVENTS)} eventos de entrada, {len(OUTPUT_EVENTS)} de salida → {OUT}")


if __name__ == "__main__":
    main()
