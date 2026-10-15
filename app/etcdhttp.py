"""Cliente mínimo do etcd v3 via gateway HTTP/JSON (só biblioteca padrão).

O etcd expõe a API v3 também em HTTP/JSON (grpc-gateway) na porta de cliente.
Chaves e valores trafegam em base64. Assim não precisamos de pip install.
"""
import base64
import json
import os
import time
import urllib.error
import urllib.request

DEFAULT_EPS = "http://172.31.0.31:2379,http://172.31.0.32:2379,http://172.31.0.33:2379"


def b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def unb64(s: str) -> str:
    return base64.b64decode(s).decode() if s else ""


def prefix_end(prefix: str) -> str:
    """range_end que cobre todas as chaves com o prefixo (como etcdctl --prefix)."""
    b = bytearray(prefix.encode())
    b[-1] += 1
    return base64.b64encode(bytes(b)).decode()


class Etcd:
    def __init__(self, endpoints=None, timeout=2.0):
        eps = endpoints or os.environ.get("ETCD_ENDPOINTS", DEFAULT_EPS)
        self.endpoints = [e.strip().rstrip("/") for e in eps.split(",") if e.strip()]
        self.timeout = timeout
        self.atual = 0  # índice do endpoint em uso (failover circular)

    def _post(self, path, body, timeout=None):
        ultimo_erro = None
        for _ in range(len(self.endpoints)):
            ep = self.endpoints[self.atual]
            req = urllib.request.Request(
                ep + path, data=json.dumps(body).encode(),
                headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=timeout or self.timeout) as r:
                    return json.loads(r.read() or b"{}")
            except (urllib.error.URLError, OSError, TimeoutError) as e:
                ultimo_erro = f"{ep}: {e}"
                self.atual = (self.atual + 1) % len(self.endpoints)
        raise ConnectionError(ultimo_erro)

    def put(self, key, value):
        r = self._post("/v3/kv/put", {"key": b64(key), "value": b64(value)})
        return int(r["header"]["revision"])

    def get(self, key, serializable=False):
        r = self._post("/v3/kv/range", {"key": b64(key), "serializable": serializable})
        kvs = r.get("kvs", [])
        if not kvs:
            return None, int(r["header"]["revision"])
        return unb64(kvs[0].get("value", "")), int(kvs[0]["mod_revision"])

    def get_prefix(self, prefix):
        r = self._post("/v3/kv/range", {"key": b64(prefix), "range_end": prefix_end(prefix)})
        return {unb64(kv["key"]): unb64(kv.get("value", "")) for kv in r.get("kvs", [])}, \
            int(r["header"]["revision"])

    def watch_prefix(self, prefix, start_revision=None):
        """Gerador de eventos (tipo, chave, valor, revisão). Reconecta em outro
        endpoint se a conexão cair, retomando de last_rev+1 (sem perder eventos)."""
        prox = start_revision
        while True:
            ep = self.endpoints[self.atual]
            req_body = {"create_request": {"key": b64(prefix), "range_end": prefix_end(prefix),
                                           "progress_notify": True}}
            if prox:
                req_body["create_request"]["start_revision"] = prox
            req = urllib.request.Request(
                ep + "/v3/watch", data=json.dumps(req_body).encode(),
                headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=15) as resp:
                    yield ("conectado", ep, None, prox)
                    for linha in resp:
                        if not linha.strip():
                            continue
                        res = json.loads(linha).get("result", {})
                        eventos = res.get("events", [])
                        if not eventos and not res.get("created") and res.get("header", {}).get("revision"):
                            # progress notify: nada mudou até essa revisão
                            prox = max(prox or 0, int(res["header"]["revision"]) + 1)
                        for ev in eventos:
                            kv = ev["kv"]
                            rev = int(kv["mod_revision"])
                            prox = rev + 1
                            yield (ev.get("type", "PUT"), unb64(kv["key"]),
                                   unb64(kv.get("value", "")), rev)
                yield ("desconectado", ep, "conexão encerrada pelo servidor", prox)
            except (urllib.error.URLError, OSError, ValueError) as e:
                yield ("desconectado", ep, str(e), prox)
            self.atual = (self.atual + 1) % len(self.endpoints)
            time.sleep(0.5)
