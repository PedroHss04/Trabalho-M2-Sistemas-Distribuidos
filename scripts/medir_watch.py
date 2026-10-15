"""E9 (parte 1): mede o tempo entre um put em /config/ e a reação de cada app.

Uso (dentro do contêiner cliente):
    python /scripts/medir_watch.py [repeticoes] [apps]
    python /scripts/medir_watch.py 10 app1,app2,app3

Grava um CSV em /resultados/watch_<data>.csv.
"""
import csv
import json
import os
import statistics
import sys
import time

sys.path.insert(0, "/app")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
from etcdhttp import Etcd  # noqa: E402

REPS = int(sys.argv[1]) if len(sys.argv) > 1 else 10
APPS = (sys.argv[2] if len(sys.argv) > 2 else "app1,app2,app3").split(",")
SAIDA = os.environ.get("RESULTADOS", "/resultados")


def main():
    etcd = Etcd()
    linhas = []
    for i in range(1, REPS + 1):
        t0 = time.time()
        rev = etcd.put("/config/modo", json.dumps({"valor": f"teste-{i}", "ts": t0}))
        pendentes = set(APPS)
        prazo = t0 + 10
        while pendentes and time.time() < prazo:
            acks, _ = etcd.get_prefix("/acks/")
            for app in list(pendentes):
                a = acks.get(f"/acks/{app}")
                if a and json.loads(a)["rev"] >= rev:
                    ms = 1000 * (json.loads(a)["recebido"] - t0)
                    linhas.append({"repeticao": i, "revisao": rev, "app": app, "atraso_ms": round(ms, 2)})
                    pendentes.discard(app)
            time.sleep(0.02)
        if pendentes:
            print(f"  rep {i}: sem resposta de {sorted(pendentes)} em 10 s")
        print(f"rep {i:2d} rev {rev}: " + "  ".join(
            f"{l['app']}={l['atraso_ms']:.1f}ms" for l in linhas if l["repeticao"] == i))
        time.sleep(0.5)

    print("\nResumo (atraso put -> app aplicou):")
    print(f"{'app':6} {'n':>3} {'média ms':>9} {'desvio ms':>10} {'mín':>7} {'máx':>7}")
    for app in APPS:
        v = [l["atraso_ms"] for l in linhas if l["app"] == app]
        if v:
            dp = statistics.stdev(v) if len(v) > 1 else 0.0
            print(f"{app:6} {len(v):3d} {statistics.mean(v):9.2f} {dp:10.2f} {min(v):7.2f} {max(v):7.2f}")

    os.makedirs(SAIDA, exist_ok=True)
    arq = os.path.join(SAIDA, f"watch_{time.strftime('%Y%m%d_%H%M%S')}.csv")
    with open(arq, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["repeticao", "revisao", "app", "atraso_ms"])
        w.writeheader()
        w.writerows(linhas)
    print(f"\nCSV salvo em {arq}")


if __name__ == "__main__":
    main()
