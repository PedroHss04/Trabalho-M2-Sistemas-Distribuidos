"""Escritor contínuo: prova (ou refuta) que não houve indisponibilidade.

Faz um put em /disponibilidade/contador a cada INTERVALO segundos e registra
sucesso, falha e latência. Rode-o em um terminal enquanto executa a
reconfiguração (scripts/e9_reconfig.sh) em outro.

Uso (dentro do contêiner cliente):
    python /scripts/escritor.py [duracao_s] [intervalo_s]
    python /scripts/escritor.py 120 0.05      # Ctrl+C encerra antes e mostra o resumo
"""
import csv
import os
import statistics
import sys
import time

sys.path.insert(0, "/app")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
from etcdhttp import Etcd  # noqa: E402

DURACAO = float(sys.argv[1]) if len(sys.argv) > 1 else 120
INTERVALO = float(sys.argv[2]) if len(sys.argv) > 2 else 0.05
SAIDA = os.environ.get("RESULTADOS", "/resultados")


def main():
    etcd = Etcd(timeout=1.0)
    regs = []  # (t, ok, latencia_ms, endpoint, erro)
    inicio = time.time()
    seg_atual, ok_seg, falha_seg, max_seg = int(inicio), 0, 0, 0.0
    i = 0
    try:
        while time.time() - inicio < DURACAO:
            i += 1
            t = time.time()
            try:
                etcd.put("/disponibilidade/contador", str(i))
                lat = 1000 * (time.time() - t)
                regs.append((t, True, lat, etcd.endpoints[etcd.atual], ""))
                ok_seg += 1
                max_seg = max(max_seg, lat)
            except ConnectionError as e:
                regs.append((t, False, 1000 * (time.time() - t), "", str(e)[:120]))
                falha_seg += 1
            if int(time.time()) != seg_atual:
                marca = "  <-- FALHAS" if falha_seg else ""
                print(f"{time.strftime('%H:%M:%S')}  ok={ok_seg:3d}  falhas={falha_seg:2d}  "
                      f"lat_max={max_seg:7.1f} ms{marca}", flush=True)
                seg_atual, ok_seg, falha_seg, max_seg = int(time.time()), 0, 0, 0.0
            time.sleep(INTERVALO)
    except KeyboardInterrupt:
        pass

    oks = [r[2] for r in regs if r[1]]
    falhas = [r for r in regs if not r[1]]
    # maior janela sem nenhuma escrita bem-sucedida
    tempos_ok = [r[0] for r in regs if r[1]]
    maior_gap = max((b - a for a, b in zip(tempos_ok, tempos_ok[1:])), default=0) * 1000
    print("\n===== Resumo do escritor =====")
    print(f"escritas: {len(regs)}  ok: {len(oks)}  falhas: {len(falhas)}")
    if oks:
        q = statistics.quantiles(oks, n=100) if len(oks) >= 100 else None
        print(f"latência ms: média {statistics.mean(oks):.1f}  desvio {statistics.pstdev(oks):.1f}  "
              f"máx {max(oks):.1f}" + (f"  p99 {q[98]:.1f}" if q else ""))
    print(f"maior intervalo entre duas escritas OK: {maior_gap:.0f} ms "
          f"(intervalo nominal {INTERVALO * 1000:.0f} ms)")
    for r in falhas[:5]:
        print(f"  falha às {time.strftime('%H:%M:%S', time.localtime(r[0]))}: {r[4]}")
    # linha legível por máquina (consumida por scripts/e9_repeticoes.sh)
    print(f"RESUMO;{len(regs)};{len(oks)};{len(falhas)};{maior_gap:.0f};"
          f"{statistics.mean(oks) if oks else 0:.1f};{max(oks) if oks else 0:.1f}")

    os.makedirs(SAIDA, exist_ok=True)
    arq = os.path.join(SAIDA, f"escritor_{time.strftime('%Y%m%d_%H%M%S')}.csv")
    with open(arq, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "ok", "latencia_ms", "endpoint", "erro"])
        w.writerows([(f"{r[0]:.3f}", int(r[1]), f"{r[2]:.2f}", r[3], r[4]) for r in regs])
    print(f"CSV salvo em {arq}")


if __name__ == "__main__":
    main()
