"""Aplicação de exemplo que reage a mudanças de configuração via watch (Ênfase C).

- Ao iniciar, lê toda a configuração em /config/ (estado inicial).
- Depois abre um watch a partir da revisão seguinte: nenhuma mudança se perde,
  mesmo que o membro do etcd caia (reconecta em outro e retoma de last_rev+1).
- Se o membro perder o líder (partição), o etcd encerra o watch (require leader)
  e a app troca de membro, retomando da mesma revisão.
- A cada mudança, "aplica" a configuração e grava um ACK em /acks/<app>,
  que o scripts/medir_watch.py usa para medir o tempo de propagação.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from etcdhttp import Compactado, Etcd  # noqa: E402

NOME = os.environ.get("APP_NAME", "app")
PREFIXO = "/config/"


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} [{NOME}] {msg}", flush=True)


def ler_estado(etcd):
    while True:
        try:
            return etcd.get_prefix(PREFIXO)
        except ConnectionError as e:
            log(f"etcd indisponível ({e}); tentando de novo")
            time.sleep(1)


def main():
    etcd = Etcd()
    config, rev = ler_estado(etcd)
    log(f"configuração inicial (revisão {rev}): {config}")
    while True:
        try:
            acompanhar(etcd, config, rev)
        except Compactado as e:
            # o histórico entre a última revisão vista e a compactação se perdeu:
            # relê o estado inteiro (snapshot) e volta a acompanhar dali em diante
            config, rev = ler_estado(etcd)
            log(f"watch cancelado ({e}); estado relido na revisão {rev}: {config}")


def acompanhar(etcd, config, rev):
    for tipo, chave, valor, revisao in etcd.watch_prefix(PREFIXO, start_revision=rev + 1):
        if tipo == "conectado":
            log(f"watch aberto em {chave} a partir da revisão {revisao}")
            continue
        if tipo == "desconectado":
            log(f"watch caiu em {chave} ({valor}); reconectando e retomando da revisão {revisao}")
            continue

        agora = time.time()
        if tipo == "DELETE":
            config.pop(chave, None)
            log(f"rev={revisao} REMOVIDA {chave}")
        else:
            config[chave] = valor
            atraso = ""
            try:
                ts = json.loads(valor).get("ts")
                if ts:
                    atraso = f"  (atraso {1000 * (agora - ts):.1f} ms)"
            except (ValueError, AttributeError):
                pass
            log(f"rev={revisao} APLICADA {chave} = {valor}{atraso}")
        try:
            etcd.put(f"/acks/{NOME}", json.dumps({"rev": revisao, "recebido": agora}))
        except ConnectionError as e:
            log(f"não consegui gravar ACK: {e}")


if __name__ == "__main__":
    main()
