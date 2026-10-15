"""Sonda de medição: roda DENTRO do contêiner cliente (python /scripts/sonda.py ...).

Todas as medições usam um único relógio (o do contêiner) e consultam os membros em
threads paralelas a cada ~10 ms. Assim a precisão não depende do custo do docker exec
(~200 ms no Windows) nem de membros mortos travando a consulta dos vivos.

Subcomandos:
  lider                      imprime o nome do líder atual
  status                     tabela: membro, líder?, termo, índice, aplicado
  esperar-saudavel [T]       espera os membros vivos concordarem num líder (até T s)
  eleicao [T]                espera o líder atual sumir e outro assumir; mede o intervalo
  sincronizar NOME [T] [tol] espera NOME (que volta de falha/partição) alcançar o líder
  caminho [N]                E2: latência de put direto no líder x via seguidor
  propagacao [N]             E3: put no líder -> valor visível em cada seguidor (N vezes)
  leituras [N] [NOME]        E4: latência de get linearizável x serializável em NOME
"""
import csv
import json
import os
import statistics
import sys
import threading
import time

sys.path.insert(0, "/app")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
from etcdhttp import Etcd, b64, unb64  # noqa: E402

MEMBROS = {f"etcd{i}": f"http://172.31.0.3{i}:2379" for i in range(1, 5)}
SAIDA = os.environ.get("RESULTADOS", "/resultados")
etcd = Etcd(",".join(MEMBROS.values()), timeout=1.0)


def ms(seg):
    return round(1000 * seg, 1)


def stamp():
    return time.strftime("%Y%m%d_%H%M%S")


def resumo(nome, valores):
    if not valores:
        return f"{nome}: sem amostras"
    dp = statistics.stdev(valores) if len(valores) > 1 else 0.0
    return (f"{nome}: n={len(valores)}  média={statistics.mean(valores):.2f}  desvio={dp:.2f}  "
            f"mín={min(valores):.2f}  máx={max(valores):.2f}")


def salvar_csv(prefixo, campos, linhas):
    os.makedirs(SAIDA, exist_ok=True)
    arq = os.path.join(SAIDA, f"{prefixo}_{stamp()}.csv")
    with open(arq, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        w.writerows(linhas)
    print(f"CSV salvo em {arq}")


class Observador:
    """Uma thread por membro, guardando o último status (ou None se não respondeu)."""

    def __init__(self, nomes=MEMBROS, intervalo=0.01):
        self.ult = {n: None for n in nomes}     # nome -> (t, status|None)
        self.vivo_em = {n: None for n in nomes}  # último instante em que respondeu
        self._parar = False
        self.intervalo = intervalo
        self.threads = [threading.Thread(target=self._loop, args=(n,), daemon=True) for n in nomes]
        for t in self.threads:
            t.start()
        time.sleep(0.3)

    def _loop(self, nome):
        while not self._parar:
            try:
                s = etcd.status_de(MEMBROS[nome], timeout=0.3)
                t = time.time()
                self.ult[nome] = (t, s)
                self.vivo_em[nome] = t
            except Exception:
                self.ult[nome] = (time.time(), None)
            time.sleep(self.intervalo)

    def status(self, nome):
        u = self.ult.get(nome)
        return u[1] if u else None

    def nome_por_id(self, mid):
        for n in self.ult:
            s = self.status(n)
            if s and s["id"] == mid:
                return n
        return f"id:{mid}"

    def lider(self):
        """(nome, id, termo) do líder reconhecido pelo maior termo visto entre os membros vivos."""
        melhor = None
        for n in self.ult:
            s = self.status(n)
            if s and s["lider"] != "0" and (melhor is None or s["termo"] > melhor[2]):
                melhor = (self.nome_por_id(s["lider"]), s["lider"], s["termo"])
        return melhor

    def parar(self):
        self._parar = True


def cmd_lider():
    obs = Observador()
    l = obs.lider()
    obs.parar()
    if not l:
        sys.exit("sem líder")
    print(l[0])


def cmd_status():
    obs = Observador()
    print(f"{'membro':7} {'resp':4} {'líder?':6} {'learner':7} {'termo':>5} {'índice':>7} {'aplicado':>8}")
    for n in obs.ult:
        s = obs.status(n)
        if not s:
            print(f"{n:7} {'não':4}")
            continue
        print(f"{n:7} {'sim':4} {('SIM' if s['lider'] == s['id'] else '-'):6} "
              f"{('sim' if s['learner'] else '-'):7} {s['termo']:5d} {s['indice']:7d} {s['aplicado']:8d}")
    obs.parar()


def cmd_esperar_saudavel(limite=60):
    t0 = time.time()
    obs = Observador()
    while time.time() - t0 < limite:
        vivos = [obs.status(n) for n in obs.ult if obs.status(n)]
        lideres = {s["lider"] for s in vivos}
        # o líder precisa estar VIVO: logo após uma queda os seguidores ainda apontam o morto
        if len(vivos) >= 2 and len(lideres) == 1 and lideres <= {s["id"] for s in vivos}:
            print(f"saudável em {time.time() - t0:.2f} s: líder {obs.nome_por_id(lideres.pop())}, "
                  f"{len(vivos)} membros respondendo")
            obs.parar()
            return
        time.sleep(0.05)
    obs.parar()
    sys.exit("cluster não ficou saudável a tempo")


def cmd_eleicao(limite=30):
    """Mede a janela sem líder: último status OK do líder antigo -> primeiro membro a
    reportar outro líder com termo maior. Imprime 'pronto' para o script agir (kill, partição)."""
    obs = Observador()
    antigo = obs.lider()
    if not antigo:
        sys.exit("sem líder inicial")
    nome0, id0, termo0 = antigo
    print(f"pronto: líder atual {nome0} (termo {termo0})", flush=True)
    t0 = time.time()
    while time.time() - t0 < limite:
        for n in obs.ult:
            u = obs.ult[n]
            s = u[1] if u else None
            if s and s["lider"] not in ("0", id0) and s["termo"] > termo0:
                t_novo = u[0]
                ult_ok = obs.vivo_em[nome0] or t0
                novo = obs.nome_por_id(s["lider"])
                time.sleep(0.3)  # deixa os outros membros reconhecerem o novo líder
                print(json.dumps({"lider_antigo": nome0, "termo_antigo": termo0,
                                  "lider_novo": novo, "termo_novo": s["termo"],
                                  "visto_por": n,
                                  "sem_lider_ms": ms(t_novo - ult_ok),
                                  "desde_pronto_s": round(t_novo - t0, 3)}), flush=True)
                obs.parar()
                return
        time.sleep(0.005)
    obs.parar()
    sys.exit("nenhum novo líder no prazo")


def cmd_sincronizar(nome, limite=60, tolerancia=0):
    """Membro NOME volta: mede (a) quando ele adota o termo atual (reingresso) e
    (b) quando o RAFT APPLIED INDEX dele alcança o do líder (sincronizado).
    tolerancia: entradas de folga (útil quando há escritas contínuas, como no E9)."""
    obs = Observador()
    s0 = obs.status(nome)
    termo_ini = s0["termo"] if s0 else -1
    print(f"pronto: {nome} termo={termo_ini} aplicado={s0['aplicado'] if s0 else '?'}", flush=True)
    t0 = time.time()
    t_reingresso = None
    while time.time() - t0 < limite:
        l = obs.lider()
        s = obs.status(nome)
        ref = obs.status(l[0]) if l and l[0] in obs.ult else None
        if s and ref and l[0] != nome:
            if t_reingresso is None and (s["termo"] != termo_ini or s["lider"] == l[1]) \
                    and s["termo"] == l[2]:
                t_reingresso = obs.ult[nome][0]
            if t_reingresso and s["aplicado"] >= ref["aplicado"] - tolerancia:
                t_sync = obs.ult[nome][0]
                print(json.dumps({"membro": nome, "lider": l[0], "termo": l[2],
                                  "aplicado_membro": s["aplicado"], "aplicado_lider": ref["aplicado"],
                                  "reingresso_s": round(t_reingresso - t0, 3),
                                  "sincronizou_apos_reingresso_ms": ms(t_sync - t_reingresso),
                                  "total_desde_pronto_s": round(t_sync - t0, 3)}), flush=True)
                obs.parar()
                return
        time.sleep(0.005)
    obs.parar()
    sys.exit(f"{nome} não sincronizou no prazo")


def get_valor(ep, chave, serializavel):
    r = etcd._post_em(ep, "/v3/kv/range", {"key": b64(chave), "serializable": serializavel})
    kvs = r.get("kvs", [])
    return unb64(kvs[0].get("value", "")) if kvs else None


def cmd_propagacao(n=20):
    """put no líder; threads leem (serializável, dado LOCAL) cada seguidor até ver o valor."""
    obs = Observador()
    l = obs.lider()
    obs.parar()
    lider = l[0]
    seguidores = [m for m in ("etcd1", "etcd2", "etcd3") if m != lider]
    print(f"líder: {lider}; seguidores: {seguidores}")
    linhas = []
    for i in range(1, n + 1):
        valor = f"v{i}-{time.time()}"
        vistos = {}

        def esperar(seg, t_put):
            while time.time() - t_put < 5:
                if get_valor(MEMBROS[seg], "/e3/propagacao", True) == valor:
                    vistos[seg] = time.time() - t_put
                    return

        t_put = time.time()
        threads = [threading.Thread(target=esperar, args=(s, t_put)) for s in seguidores]
        for t in threads:
            t.start()
        etcd._post_em(MEMBROS[lider], "/v3/kv/put", {"key": b64("/e3/propagacao"), "value": b64(valor)})
        lat_put = time.time() - t_put
        for t in threads:
            t.join()
        lin = {"repeticao": i, "put_ms": ms(lat_put)}
        for s in seguidores:
            lin[f"visivel_{s}_ms"] = ms(vistos[s]) if s in vistos else ""
        linhas.append(lin)
        print(f"rep {i:2d}: put={lin['put_ms']:6.1f} ms  " +
              "  ".join(f"{s} vê em {lin[f'visivel_{s}_ms']} ms" for s in seguidores))
        time.sleep(0.2)
    print()
    print(resumo("put no líder (ms)", [x["put_ms"] for x in linhas]))
    for s in seguidores:
        print(resumo(f"visível em {s} (ms)", [x[f"visivel_{s}_ms"] for x in linhas
                                              if x[f"visivel_{s}_ms"] != ""]))
    salvar_csv("e3_propagacao", list(linhas[0].keys()), linhas)


def cmd_caminho(n=100):
    """E2: put direto no líder x put num seguidor (que encaminha a proposta ao líder).
    A diferença de latência é o custo do salto extra seguidor -> líder."""
    obs = Observador()
    lider = obs.lider()[0]
    obs.parar()
    seguidor = next(m for m in ("etcd1", "etcd2", "etcd3") if m != lider)
    direto, via = [], []
    for i in range(n):
        for ep, lista in ((MEMBROS[lider], direto), (MEMBROS[seguidor], via)):
            t = time.time()
            etcd._post_em(ep, "/v3/kv/put", {"key": b64("/e2/caminho"), "value": b64(str(i))})
            lista.append(1000 * (time.time() - t))
    print(f"put com endpoint único ({n} de cada, alternados); líder={lider}, seguidor={seguidor}")
    print(resumo(f"  direto no líder {lider} (ms)", direto))
    print(resumo(f"  via seguidor {seguidor} (ms)", via))
    salvar_csv("e2_caminho", ["i", "direto_lider_ms", "via_seguidor_ms"],
               [{"i": i, "direto_lider_ms": round(a, 3), "via_seguidor_ms": round(b, 3)}
                for i, (a, b) in enumerate(zip(direto, via), 1)])


def cmd_leituras(n=200, nome="etcd2"):
    """get linearizável (ReadIndex: confirma com o líder) x serializável (local) no membro NOME."""
    ep = MEMBROS[nome]
    etcd._post_em(ep, "/v3/kv/put", {"key": b64("/e4/x"), "value": b64("1")})
    lin, ser = [], []
    for _ in range(n):
        t = time.time()
        get_valor(ep, "/e4/x", False)
        lin.append(1000 * (time.time() - t))
        t = time.time()
        get_valor(ep, "/e4/x", True)
        ser.append(1000 * (time.time() - t))
    print(f"leituras em {nome} ({n} de cada, alternadas):")
    print(resumo("  linearizável (ms)", lin))
    print(resumo("  serializável (ms)", ser))
    salvar_csv(f"e4_leituras_{nome}", ["i", "linearizavel_ms", "serializavel_ms"],
               [{"i": i, "linearizavel_ms": round(a, 3), "serializavel_ms": round(b, 3)}
                for i, (a, b) in enumerate(zip(lin, ser), 1)])


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    cmd, args = sys.argv[1], sys.argv[2:]
    if cmd == "lider":
        cmd_lider()
    elif cmd == "status":
        cmd_status()
    elif cmd == "esperar-saudavel":
        cmd_esperar_saudavel(float(args[0]) if args else 60)
    elif cmd == "eleicao":
        cmd_eleicao(float(args[0]) if args else 30)
    elif cmd == "sincronizar":
        cmd_sincronizar(args[0], float(args[1]) if len(args) > 1 else 60,
                        int(args[2]) if len(args) > 2 else 0)
    elif cmd == "caminho":
        cmd_caminho(int(args[0]) if args else 100)
    elif cmd == "propagacao":
        cmd_propagacao(int(args[0]) if args else 20)
    elif cmd == "leituras":
        cmd_leituras(int(args[0]) if args else 200, args[1] if len(args) > 1 else "etcd2")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
