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


class Compactado(Exception):
    """O watch pediu uma revisão anterior à compactação do histórico (MVCC)."""

    def __init__(self, compact_revision):
        super().__init__(f"revisão compactada até {compact_revision}")
        self.compact_revision = compact_revision


class Etcd:
    def __init__(self, endpoints=None, timeout=2.0):
        eps = endpoints or os.environ.get("ETCD_ENDPOINTS", DEFAULT_EPS)
        self.endpoints = [e.strip().rstrip("/") for e in eps.split(",") if e.strip()]
        self.timeout = timeout
        self.atual = 0  # índice do endpoint em uso (failover circular)

    def _post_em(self, ep, path, body, timeout=None):
        """POST em UM endpoint, sem failover (para observar um membro específico)."""
        req = urllib.request.Request(
            ep.rstrip("/") + path, data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout or self.timeout) as r:
            return json.loads(r.read() or b"{}")

    def _post(self, path, body, timeout=None):
        ultimo_erro = None
        for _ in range(len(self.endpoints)):
            ep = self.endpoints[self.atual]
            try:
                return self._post_em(ep, path, body, timeout)
            except (urllib.error.URLError, OSError, TimeoutError) as e:
                ultimo_erro = f"{ep}: {e}"
                self.atual = (self.atual + 1) % len(self.endpoints)
        raise ConnectionError(ultimo_erro)

    def status_de(self, ep, timeout=0.5):
        """/v3/maintenance/status de UM endpoint (sem failover): líder, termo e índices do Raft."""
        s = self._post_em(ep, "/v3/maintenance/status", {}, timeout)
        return {"id": s["header"]["member_id"], "lider": s.get("leader", "0"),
                "termo": int(s.get("raftTerm", 0)), "indice": int(s.get("raftIndex", 0)),
                "aplicado": int(s.get("raftAppliedIndex", 0)), "learner": bool(s.get("isLearner"))}

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
        falhas = 0  # tentativas seguidas sem conseguir abrir o watch (backoff exponencial)
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
                    falhas = 0
                    yield ("conectado", ep, None, prox)
                    for linha in resp:
                        if not linha.strip():
                            continue
                        res = json.loads(linha).get("result", {})
                        if res.get("canceled"):
                            # revisão pedida já foi compactada: o histórico não existe mais
                            # e quem chamou precisa reler o estado completo (get) e reabrir o watch
                            raise Compactado(int(res.get("compact_revision", 0)))
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
            # 0,5 s na primeira tentativa (failover rápido), dobrando até 5 s se nenhum membro responde
            time.sleep(min(0.5 * 2 ** (falhas // len(self.endpoints)), 5))
            falhas += 1
