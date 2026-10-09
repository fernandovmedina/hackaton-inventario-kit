"""Chequeo de mutantes: mete un bug a propósito en la referencia y confirma que la prueba
de contrato correspondiente falla. Detener antes cualquier otro servicio de inventario
conectado al broker (las pruebas leen todo inventory.events).

    python3 organizer/reference/mutation_check.py              # todos
    python3 organizer/reference/mutation_check.py desorden ... # solo esos mutantes
"""
import subprocess, sys, tempfile, time, urllib.request, os, signal
from pathlib import Path
K = str(Path(__file__).resolve().parents[2])
S = tempfile.mkdtemp(prefix="mutantes-")
src = open(f"{K}/organizer/reference/inventory.py").read()
MUTANTS = {
  "sin_idempotencia": ('            if event["event_id"] in self.seen:\n                return\n', '', "idempotencia"),
  "suma_en_vez_de_reemplazo": ('            need.required, need.inspection_id', '            required = (need.required or 0) + (required or 0)\n            need.required, need.inspection_id', "necesidad_se_reemplaza"),
  "cuenta_compras_de_cliente": ('        if d["purpose"] != "restock":\n            return\n', '', "compra_para_cliente"),
  "permite_negativos": ('if self.on_hand[(part_id, loc)] < qty or qty - from_reserve > self.available(part_id):', 'if False:', "salida_sin_negativos"),
  "pierde_eventos_desordenados": ('                self.pending.append(event)\n                return', '                return', "desorden"),
  "ignora_anulacion": ('        elif t == "inspection.voided":', '        elif t == "inspection.voided_x":', "rechazo_y_anulacion"),
  "reserva_repair": ('buys = [it for it in d["items"] if it["action"] == "buy"]', 'buys = [it for it in d["items"] if it["action"] in ("buy", "repair")]', "lineas_que_no_reservan"),
  "no_surte_con_compra": ('        for need in waiting:\n', '        for need in waiting[:0]:\n', "reserva_faltante_compra_y_salida"),
}
results = {}
for i, (name, (old, new, test)) in enumerate((k,v) for k,v in MUTANTS.items() if k in sys.argv[1:] or len(sys.argv)==1):
    assert old in src, name
    path = f"{S}/mut_{name}.py"; open(path, "w").write(src.replace(old, new, 1))
    port = 8100 + i
    proc = subprocess.Popen(["uv","run","-q","--python","3.12","--with","confluent-kafka==2.6.1",path,"--port",str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    for _ in range(60):
        try: urllib.request.urlopen(f"http://localhost:{port}/reorder-suggestions", timeout=1); break
        except Exception: time.sleep(1)
    time.sleep(45)  # que termine de releer los tópicos
    r = subprocess.run(["uv","run","-q","--python","3.12","--with","confluent-kafka==2.6.1","--with","jsonschema",f"{K}/contract-tests/run_tests.py","--api",f"http://localhost:{port}","--only",test,"--timeout","10"], capture_output=True, text=True)
    os.killpg(proc.pid, signal.SIGTERM)
    line = next((l for l in r.stdout.splitlines() if test in l), r.stdout[-300:])
    results[name] = ("DETECTADO" if "✘" in line else "NO DETECTADO") + " · " + line.strip()[:150]
    print(f"{name}: {results[name]}", flush=True)
