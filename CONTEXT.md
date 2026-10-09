# CONTEXT: guía del organizador

> **Solo organizadores. No se reparte.** `organizer/package.sh` deja fuera este archivo y la
> carpeta `organizer/`.

Esta guía explica qué es cada parte del kit, qué se entrega a los equipos y cómo evaluarlos
al terminar el reto, paso a paso.

---

## 1. La idea en una línea

El kit simula el taller: publica eventos (piezas, órdenes, inspecciones, compras) en un broker
compatible con Kafka. Cada equipo construye un **servicio de inventario** que consume esos
eventos, publica los suyos en `inventory.events` y expone una API HTTP. Tú evalúas ese
servicio con pruebas automáticas y una demo.

```
 Simulador del taller ──► shop.catalog / shop.work_orders / shop.inspections / shop.purchasing
   (panel :8090)                                │
                                                ▼
                                   Servicio del EQUIPO (API :8000)
                                                │
                                                ▼
                                inventory.events  (+ inventory.dlq)
                                                │
                       Pruebas de contrato ◄────┘  (también llaman a la API)
```

---

## 2. Mapa del repositorio

| Ruta | Qué es | ¿Se reparte? |
|---|---|---|
| `README.md` | Cómo levantar el kit (para los equipos) | Sí |
| `contract/` | El contrato: JSON Schema por evento, `openapi.yaml` y las 23 reglas + estándar de etiquetas (`contract/README.md`) | Sí |
| `data/events.jsonl` | 6 semanas de operación (1,742 entregas, con duplicados y desorden) | Sí |
| `simulator/` | Reproduce el dataset en el broker y sirve el panel de la demo | Sí |
| `contract-tests/` | Pruebas automáticas, dan el puntaje sobre 50 | Sí |
| `labels/etiquetas-demo.pdf` | 22 etiquetas Code 128 para la demo | Sí |
| `tools/validate_events.py` | Valida un archivo de eventos contra el contrato | Sí |
| `docker-compose.yml` | Redpanda + consola + simulador (+ pruebas con perfil `tests`) | Sí |
| `CONTEXT.md` | Esta guía | **No** |
| `organizer/` | Export del catálogo real, generadores, solución de referencia, chequeo de mutantes, empaquetado | **No** |

Puertos que usa el kit: `19092` (broker), `8080` (consola), `8090` (panel del simulador). El
servicio de cada equipo debe escuchar en `8000` (o se pasa otro con `--api`).

---

## 3. Antes del evento

### 3.1 Preparar el paquete

```bash
cd hackathon-inventario-kit
git add -A && git commit -m "kit del hackathon"     # package.sh empaqueta lo versionado
organizer/package.sh                                 # → dist/hackathon-inventario-kit.zip
shasum -a 256 dist/hackathon-inventario-kit.zip contract-tests/run_tests.py
```

Guarda los dos hashes: el de `run_tests.py` sirve para comprobar el día de la evaluación que
nadie modificó las pruebas (paso 5.1).

### 3.2 Ensayo completo con la solución de referencia

Hazlo al menos una vez en la máquina donde vas a evaluar. Así confirmas que todo funciona y
sabes cuánto tarda.

```bash
# 1. Kit arriba, replay sin pausas (2-3 s en vez de 17 min)
RESET=1 SPEED=0 docker compose up -d --build
docker compose logs simulator | tail -3          # debe decir "replay terminado: 1742 eventos"

# 2. La referencia hace el papel de un equipo (otra terminal; Python 3.12 obligatorio)
uv run --python 3.12 --with confluent-kafka==2.6.1 organizer/reference/inventory.py --port 8000
#    espera ~30-60 s a que relea los tópicos

# 3. Pruebas: debe salir "Total: 50.0 de 50 puntos" (~40 s)
docker compose run --rm contract-tests --api http://host.docker.internal:8000

# 4. Reconstrucción (ver paso 5.3): snapshot → reiniciar la referencia → compare = "✔ mismo saldo"
```

Para comprobar que las pruebas detectan errores (opcional, ~8 min), detén antes la referencia
buena y corre:

```bash
python3 organizer/reference/mutation_check.py      # los 8 mutantes deben salir DETECTADO
```

### 3.3 Material físico

- Imprime `labels/etiquetas-demo.pdf` en hojas Avery 5162 (o en papel normal y recorta): un
  juego por mesa de demo, más uno de repuesto.
- Prueba 3 o 4 etiquetas impresas con un celular, por ejemplo con la cámara del ERP en
  `/operador` o cualquier lector de Code 128. Las más difíciles son `ARANDELA CON SELLOS 8 MM`
  y `95.600.057.2.6 V01`, por largas.
- Pega las etiquetas de ubicación (`U-100`…`U-112`) en cajas o estantes de la mesa de demo y
  ten a la mano las de pieza.

### 3.4 Qué entregar a los equipos al arrancar

1. `dist/hackathon-inventario-kit.zip`.
2. El documento técnico del reto (el doc de Claude o su PDF exportado **actualizado**).
3. Acceso a una hoja de etiquetas impresa (o el PDF para escanear desde pantalla).
4. Requisitos en sus laptops: Docker Desktop y, si van a correr las pruebas sin Docker,
   Python 3.12 (`confluent-kafka` no tiene versión para 3.14).

Diles explícitamente:

- "Empiecen leyendo `contract/README.md`; si algo contradice al documento, gana el contrato".
- "Corran las pruebas cuantas veces quieran: `docker compose run --rm contract-tests --api ...`".
- "La evaluación final usa una copia limpia de las pruebas".

---

## 4. Durante el reto

No hay nada que operar de tu lado: cada equipo corre su propio kit en su laptop. Preguntas
típicas y respuesta corta:

| Pregunta | Respuesta |
|---|---|
| "Las compras no coinciden con nada al reiniciar" | Regla 22: consumir `shop.catalog` completo antes que lo demás |
| "¿Cuánto reservo si `quantity` es null?" | Regla 5: el `qty_per_unit` de la línea; si también es null, faltante sin cantidad |
| "¿La segunda inspección suma?" | Regla 2: reemplaza, por (orden, `bom_line_id`) |
| "¿Dónde registro la entrada de una compra?" | Regla 11: `U-100` (`location_id` 100) |
| "¿Qué codifico en mis etiquetas?" | §5 del contrato: Code 128 con `U-###`, `sku`, o `P-<part_id>` |
| "Las pruebas ven eventos raros" | Solo un servicio de inventario conectado al broker mientras corren |
| "Quiero empezar de cero" | `RESET=1 docker compose up -d` borra y recrea los tópicos |
| "Quiero el replay rápido" | `SPEED=0 docker compose up -d` |

---

## 5. Evaluación de cada equipo (paso a paso)

Calcula unos **15 minutos por equipo**: 5 de preparación, 1 de pruebas, 3 de reconstrucción y
5 de demo.

Trabaja en la laptop del equipo, con su kit ya levantado y su servicio en el puerto 8000.

### 5.1 Asegurar pruebas limpias

Las pruebas del equipo pueden estar modificadas. Antes de correrlas:

```bash
cd <carpeta del kit del equipo>
shasum -a 256 contract-tests/run_tests.py        # compáralo con el hash del paso 3.1
```

Si no coincide, reemplaza `contract/` y `contract-tests/` con las del zip oficial (en una USB) y
reconstruye la imagen de pruebas:

```bash
docker compose --profile tests build contract-tests
```

### 5.2 Pruebas automáticas (50 pts)

```bash
docker compose run --rm contract-tests --api http://host.docker.internal:8000
```

- Tarda unos 40 s si el servicio responde bien. Con fallos tarda más, porque cada espera
  agota su timeout de 20 s.
- Al final imprime `Total: X de 50 puntos`: **ese es el puntaje**. Viene de 25 × (escenarios de
  libro mayor que pasan / 5) + 25 × (escenarios de integración que pasan / 9).
- Si aparece la sección "Eventos de salida que no cumplen el contrato", anótala: el equipo
  publica eventos mal formados o con una `key` equivocada. Propuesta: descuento de 2 puntos,
  o que no cuente si lo corrigen en sitio.
- Si sale `la API no responde`, revisa que su servicio escuche en `0.0.0.0:8000` y no solo en
  `127.0.0.1` dentro de un contenedor.
- Si un escenario falla por tiempo pero el equipo jura que funciona, repítelo con más
  margen: `--only <escenario> --timeout 40`. Un servicio lento no es un servicio roto.

Guarda la salida completa (cópiala o redirígela con `> equipo-X.txt`).

### 5.3 Reconstrucción

Es la regla "borrando la base y releyendo los tópicos, deben llegar al mismo saldo". Hazla
**antes de la demo**: la demo mueve existencias del dataset con salidas escaneadas, que no
viven en los tópicos.

```bash
docker compose run --rm -v "$PWD:/out" contract-tests snapshot --out /out/saldo.json --api http://host.docker.internal:8000
#  → "256 piezas guardadas en /out/saldo.json"

#  El equipo borra su base y reinicia su servicio para que relea los tópicos desde el inicio.

docker compose run --rm -v "$PWD:/out" contract-tests compare --out /out/saldo.json --api http://host.docker.internal:8000
#  → "✔ mismo saldo en 256 piezas"   o   "✘ N piezas con saldo distinto" + ejemplos
```

`compare` espera hasta 5 minutos a que el servicio termine de releer. Solo compara la
existencia (`on_hand`) por pieza y ubicación; las reservas no, porque dependen del orden en que
se intercalen los tópicos.

Las pruebas automáticas no incluyen la reconstrucción. Propuesta: **−5 puntos** del bloque de
libro mayor si `compare` falla. Decide la regla antes del evento y anúnciala.

### 5.4 Demo en vivo (5 min): Experiencia en piso (20 pts)

Abre el panel del simulador del equipo: http://localhost:8090.

1. **Recibir equipo.** En "Abrir una orden de trabajo" elige **GROB MSP-04** (4 piezas, tienen
   etiquetas impresas) y pulsa *Recibir equipo*. Anota el código `R…` que aparece.
2. **Inspección.** En la orden, abre *Capturar inspección* y marca, por ejemplo:
   - SELLOS (`ARANDELA CON SELLOS 8 MM`): Dañada · Comprar · 1
   - TORNILLO (`TORNILLO 6X12`): Falta · 4
   - GRIPPER: Dañada · Reparar
   - FLECHA: Bien

   Pulsa *Enviar inspección rápida* y luego **Aprobar**.
3. **El equipo muestra** en su pantalla de orden: lo reservado y lo faltante. Al terminar el
   dataset quedan 4 `TORNILLO 6X12` y 1 arandela en `U-100`, pero parte puede estar
   reservada para otras órdenes: lo que no esté disponible sale como faltante, y eso también
   sirve para la demo. El GRIPPER **no** debe reservarse (es reparación).
4. **Llega la compra.** En "Compra que llega a almacén" escribe el número de parte de lo que
   faltó (ej. `TORNILLO 6X12`, o una variante como `tornillo 6x12` para ver si lo relacionan),
   cantidad, y selecciona la orden en el desplegable. Pulsa *Registrar llegada*.
5. **El equipo muestra** que el faltante se surtió y la reserva subió.
6. **Salida escaneada.** Con un celular, el equipo escanea la etiqueta de ubicación `U-100`
   y la de la pieza, y retira contra el código de la orden. El saldo debe bajar al momento.
7. (Opcional, si sobra tiempo) Escanear una transferencia `U-100 → U-101` o un conteo.

Qué calificar en piso:

| Aspecto | Pregunta |
|---|---|
| Escaneo real | ¿Leyó las etiquetas impresas con la cámara del celular, sin teclear? |
| Pocos pasos | ¿Cuántos toques para retirar o contar? |
| Vista por orden | ¿Se entiende de un vistazo qué está reservado, entregado y pendiente? |
| Robustez | ¿Aceptó `TOR, CAB, BAJA` / `7014 CTA/P4` (SKU con comas y diagonales)? |

Variantes para poner a prueba en la demo (elige una):

- **Rechazo:** en vez de aprobar, *Rechazar* con motivo, capturar otra y aprobar. Debe
  reservar solo una vez.
- **Anulación:** aprobar y luego *Anular (regresar etapa)*. La reserva debe liberarse.
- **Baja:** *Dar de baja* la orden. Reservas liberadas y faltantes cerrados.
- **Compra para cliente:** registrar la llegada con "Para cliente". No debe entrar al
  almacén.

### 5.5 Documento de decisiones (15 pts)

Máximo una página. Debe responder 4 preguntas; revisa que cada respuesta sea concreta:

| Pregunta | Respuesta buena | Respuesta floja |
|---|---|---|
| ¿Cómo relacionan compras con el catálogo? | Normalización explícita + qué pasa con SKU duplicados y números de proveedor | "Comparamos el sku" |
| ¿Cómo manejan kits? | Decisión justificada (kit, componentes o ambos) usando `group_name` | No lo mencionan |
| ¿Cómo evitan reservas dobles? | Mecanismo concreto (transacción, lock por pieza, consumidor único, versión optimista) | "Validamos el stock" |
| ¿Cómo garantizan idempotencia? | `event_id` persistido en la misma transacción que el efecto | "Ignoramos duplicados" |

### 5.6 Extras (15 pts)

Del documento técnico, nivel avanzado: pronóstico de demanda por modelo, valuación a costo
promedio (ojo: hay compras en MXN y USD), tablero de rotación, observabilidad del consumidor
(lag y errores). Pide que lo muestren en 1 minuto extra o en el repositorio.

---

## 6. Hoja de puntaje por equipo

| Criterio | Pts | Cómo | Puntaje |
|---|---|---|---|
| Correctitud del libro mayor | 25 | Pruebas (bloque libro mayor) − 5 si la reconstrucción falla | |
| Integración con el taller | 25 | Pruebas (bloque integración) | |
| Experiencia en piso | 20 | Demo 5.4 | |
| Decisiones justificadas | 15 | Documento 5.5 | |
| Extras | 15 | 5.6 | |
| **Total** | **100** | | |

---

## 7. Problemas comunes

| Síntoma | Causa / solución |
|---|---|
| `Failed to build confluent-kafka` | Python 3.14. Usar `uv run --python 3.12 ...` o las pruebas en Docker |
| `la API no responde en http://host.docker.internal:8000` | El servicio escucha solo en `127.0.0.1` de otro contenedor, o en otro puerto (`--api`) |
| Todo pasa con un servicio que sabes que está mal | Hay otro servicio de inventario (o la referencia) conectado al mismo broker |
| `compare` da 0 en todo | El servicio no consume `shop.catalog` antes que lo demás (regla 22), o aún no termina de releer |
| El panel del simulador no carga | `docker compose logs simulator`; el broker tarda unos segundos en estar *healthy* |
| Puerto 8080/8090/19092 ocupado | Otro proceso en la laptop; liberarlo o cambiar el mapeo en `docker-compose.yml` |
| Quieren empezar limpio | `RESET=1 docker compose up -d` |
| Docker no arranca | Abrir Docker Desktop y esperar a que diga *running* |

---

## 8. Regenerar el kit (si cambias algo)

Todo es reproducible desde `organizer/`:

```bash
psql "$DATABASE_URL" -X -q -t -A -f organizer/export_catalog.sql | grep '^{' > organizer/catalog.raw.json
python3 organizer/build_schemas.py                         # contract/schemas
python3 organizer/generate_events.py                       # data/events.jsonl (misma semilla = mismo archivo)
uv run --with jsonschema tools/validate_events.py data/events.jsonl
uv run --with reportlab organizer/build_labels.py          # labels/etiquetas-demo.pdf
organizer/package.sh                                       # dist/hackathon-inventario-kit.zip
```

Si cambias una regla del contrato, cambia en el mismo commit: `contract/README.md`, la
referencia (`organizer/reference/inventory.py`) y, si aplica, el escenario en
`contract-tests/run_tests.py`. Después repite el ensayo del paso 3.2.
