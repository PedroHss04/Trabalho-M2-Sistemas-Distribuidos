"""Gera N chaves de ~1 KB em /carga/ para o catch-up do learner ser mensurável.

Com o banco quase vazio, o learner alcança o líder em ~0,1 s. Com milhares de
chaves, dá para ver (e medir) o tempo de transferência do estado.

Uso (dentro do contêiner cliente):
    python /scripts/carga.py 5000
"""
import os
import sys
import time

sys.path.insert(0, "/app")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
from etcdhttp import Etcd  # noqa: E402

N = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
etcd = Etcd()
valor = "x" * 1024
t = time.time()
for i in range(N):
    etcd.put(f"/carga/{i:06d}", valor)
    if (i + 1) % 1000 == 0:
        print(f"{i + 1} chaves gravadas", flush=True)
print(f"{N} chaves (~{N // 1024} MB) em {time.time() - t:.1f} s")
