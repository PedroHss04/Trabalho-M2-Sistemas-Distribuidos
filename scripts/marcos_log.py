"""Lê o log JSON de um membro etcd (stdin) e imprime a linha do tempo dos marcos de recuperação,
em ms desde o primeiro marco, usando os carimbos "ts" do próprio etcd (um único relógio).

Uso: docker logs etcd3 2>&1 | docker exec -i cliente python /scripts/marcos_log.py
"""
import json
import sys
from datetime import datetime

MARCOS = ("starting an etcd server", "receiving database snapshot", "applied snapshot",
          "published local member to cluster through raft", "ready to serve client requests")

t0 = None
for linha in sys.stdin:
    try:
        d = json.loads(linha)
    except ValueError:
        continue
    msg = d.get("msg")
    if msg in MARCOS:
        t = datetime.fromisoformat(d["ts"].replace("Z", "+00:00"))
        t0 = t0 or t
        print(f"   +{(t - t0).total_seconds() * 1000:7.0f} ms  {msg}")
