"""Gera N chaves de ~1 KB (cada put = 1 entrada no log do Raft).

Usado para tornar mensurável o catch-up de um learner (E9) ou de um membro que
volta (E7). Com o banco quase vazio, o membro alcança o líder em ~0,1 s.

Uso (dentro do contêiner cliente):
    python /scripts/carga.py [N] [threads] [prefixo]
    python /scripts/carga.py 5000              # 5000 chaves em /carga/
    python /scripts/carga.py 12000 8 /snap/    # 12000 chaves com 8 escritores paralelos
Endpoints: variável ETCD_ENDPOINTS (padrão: os 3 membros).
"""
import os
import sys
import threading
import time

sys.path.insert(0, "/app")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
from etcdhttp import Etcd  # noqa: E402

N = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
THREADS = int(sys.argv[2]) if len(sys.argv) > 2 else 4
PREFIXO = sys.argv[3] if len(sys.argv) > 3 else "/carga/"
valor = "x" * 1024
feitas = 0
trava = threading.Lock()


def escrever(inicio):
    global feitas
    etcd = Etcd()  # um cliente por thread (failover independente)
    for i in range(inicio, N, THREADS):
        etcd.put(f"{PREFIXO}{i:06d}", valor)
        with trava:
            feitas += 1
            if feitas % 2000 == 0:
                print(f"{feitas} chaves gravadas", flush=True)


t = time.time()
ths = [threading.Thread(target=escrever, args=(k,)) for k in range(THREADS)]
for th in ths:
    th.start()
for th in ths:
    th.join()
dt = time.time() - t
print(f"{N} chaves (~{N // 1024} MB) em {dt:.1f} s ({N / dt:.0f} escritas/s, {THREADS} threads)")
