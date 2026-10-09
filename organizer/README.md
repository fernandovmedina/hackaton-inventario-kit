# organizer/: solo organizadores, NO se reparte

Genera el contrato y el dataset a partir del catálogo real, y trae una solución de
referencia para comprobar que las pruebas de contrato son correctas. `package.sh`
empaqueta el kit sin esta carpeta.

```bash
# 1. Exportar el catálogo real (solo lectura; DATABASE_URL del backend)
psql "$DATABASE_URL" -X -q -t -A -f organizer/export_catalog.sql | grep '^{' > organizer/catalog.raw.json

# 2. Regenerar los JSON Schema (fuente de verdad: build_schemas.py)
python3 organizer/build_schemas.py

# 3. Regenerar data/events.jsonl (determinista por SEED) y validarlo
python3 organizer/generate_events.py
uv run --with jsonschema tools/validate_events.py data/events.jsonl

# 4. Regenerar las etiquetas de la demo
uv run --with reportlab organizer/build_labels.py

# 5. Empaquetar para los equipos → dist/hackathon-inventario-kit.zip
organizer/package.sh
```

`catalog.raw.json` no se versiona: contiene el catálogo tal cual sale de la base.
`generation_summary.json` resume cada corrida: conteos y qué trampas se plantaron.

## Solución de referencia

`reference/inventory.py` implementa el contrato completo en memoria. Saca 50 de 50 y
reconstruye la misma existencia al releer desde cero.

```bash
docker compose up -d
uv run --python 3.12 --with confluent-kafka==2.6.1 organizer/reference/inventory.py --port 8000
docker compose run --rm contract-tests --api http://host.docker.internal:8000
```

`reference/mutation_check.py` mete 8 bugs a propósito en la referencia, uno por uno, y
confirma que la prueba correspondiente falla. Los 8 quedan detectados. Antes de correrlo,
detengan la referencia buena: las pruebas leen todo `inventory.events` y la referencia
buena taparía al mutante.

## Anonimización

No salen clientes, plantas, personas ni URLs de imágenes. Los códigos de ubicación se
renumeran a `U-###` y las claves placeholder (`CLAVE PENDIENTE`, `????`, `EQUIVALENCIAS`,
`OT XXXX`) se vuelven `sku: null`. Se excluyen la ubicación `PRB` y el modelo de prueba
`HSK 63`.
