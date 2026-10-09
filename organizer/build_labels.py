"""Genera labels/etiquetas-demo.pdf: ubicaciones y piezas de la demo en Code 128
(estándar en contract/README.md §5). Hoja carta, 2×7 etiquetas de 4"×1⅓" (Avery 5162).

    uv run --with reportlab organizer/build_labels.py
"""

from __future__ import annotations

import json
from pathlib import Path

from reportlab.graphics.barcode.code128 import Code128
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "labels" / "etiquetas-demo.pdf"

LOCATIONS = range(100, 113)  # recepción U-100 + estantes U-101…U-112
# Piezas de los 2 modelos chicos (GROB MSP-04, DOOSAN DNM400) + SKU con espacios, coma y diagonal.
PARTS = [24, 186, 189, 202, 8, 193, 153, 278, 84]

COLS, ROWS = 2, 7
LABEL_W, LABEL_H = 4 * inch, 4 / 3 * inch
LEFT, TOP, GAP_X = 0.16 * inch, 0.5 * inch, 0.19 * inch


def catalog() -> tuple[dict, dict]:
    parts, locations = {}, {}
    for raw in (ROOT / "data" / "events.jsonl").open(encoding="utf-8"):
        ev = json.loads(raw)["event"]
        if ev["event_type"] == "part.upserted":
            parts[ev["data"]["part_id"]] = ev["data"]
        elif ev["event_type"] == "location.upserted":
            locations[ev["data"]["location_id"]] = ev["data"]
    return parts, locations


def part_code(part: dict) -> str:
    return part["sku"] if part["sku"] else f"P-{part['part_id']}"


def draw_label(c: canvas.Canvas, x: float, y: float, kind: str, value: str, title: str) -> None:
    c.setFont("Helvetica", 7)
    c.drawString(x + 10, y + LABEL_H - 13, kind)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(x + 10, y + LABEL_H - 24, title[:58])
    bar = 1.1
    barcode = Code128(value, barHeight=0.48 * inch, barWidth=bar, quiet=True)
    while barcode.width > LABEL_W - 0.35 * inch:  # que quepa con zona de silencio
        bar -= 0.05
        barcode = Code128(value, barHeight=0.48 * inch, barWidth=bar, quiet=True)
    bx = x + (LABEL_W - barcode.width) / 2
    barcode.drawOn(c, bx, y + 0.3 * inch)
    c.setFont("Courier-Bold", 11)
    c.drawCentredString(x + LABEL_W / 2, y + 0.12 * inch, value)


def main() -> None:
    parts, locations = catalog()
    labels = [("UBICACIÓN", locations[lid]["code"], locations[lid]["name"]) for lid in LOCATIONS]
    labels += [("PIEZA", part_code(parts[pid]), parts[pid]["name"]) for pid in PARTS]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(OUT), pagesize=letter)
    c.setTitle("Etiquetas de la demo")
    per_page = COLS * ROWS
    for i, (kind, value, title) in enumerate(labels):
        if i and i % per_page == 0:
            c.showPage()
        slot = i % per_page
        col, row = slot % COLS, slot // COLS
        x = LEFT + col * (LABEL_W + GAP_X)
        y = letter[1] - TOP - (row + 1) * LABEL_H
        draw_label(c, x, y, kind, value, title)
    c.save()
    print(f"{len(labels)} etiquetas → {OUT}")


if __name__ == "__main__":
    main()
