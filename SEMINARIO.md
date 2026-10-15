# Roteiro do seminário (10 min + 5 min de arguição)

O roteiro exige demonstração **ao vivo** com o cluster **já no ar** no início. Os vídeos gravados
servem só de contingência: gravem cada bloco abaixo uma vez antes (por exemplo, com a Barra de
Jogo do Windows, `Win+Alt+R`).

## Antes de entrar na sala (5 min antes)

```bash
docker desktop start            # ou abrir o Docker Desktop e esperar "Engine running"
./scripts/reset.sh              # cluster limpo, 3 membros saudáveis
```

Deixem **4 terminais** abertos e com fonte grande:

| Terminal | Comando já digitado |
|---|---|
| T1 – apps | `docker compose logs -f --no-log-prefix app1 app2 app3` |
| T2 – escritor | `docker exec cliente python /scripts/escritor.py 300` |
| T3 – status | `watch -n1 ./scripts/status.sh` (WSL) ou rodar `./scripts/status.sh` quando precisar |
| T4 – comandos | livre |

## Blocos e falas

| Tempo | Bloco | Quem | O que mostrar |
|---|---|---|---|
| 0:00–1:00 | 1. Problema e artigo | ⟪integrante 1⟫ | etcd = "cérebro" do Kubernetes; máquina de estados replicada; Raft (Ongaro & Ousterhout, 2014) |
| 1:00–3:00 | 2. Arquitetura | ⟪integrante 2⟫ | diagrama da seção 3.2 do relatório: 2 redes, 3 membros, líder, apps com watch |
| 3:00–7:00 | 3. Demonstração ao vivo | ⟪integrante 3⟫ | E2, E3, E5/E6, E7 (abaixo) |
| 7:00–9:00 | 4. Ênfase C (E9) | ⟪integrante 4⟫ | watch nas apps + reconfiguração com o escritor rodando |
| 9:00–10:00 | 5. Quadro-síntese | todos | CP, maioria, Raft, revisão, snapshot |

### Bloco 3 – demonstração (T4)

```bash
# E2: put num seguidor; a resposta traz o termo do líder; índice sobe nos três
docker exec etcd2 etcdctl --endpoints=http://127.0.0.1:2379 put /config/modo manutencao -w json
./scripts/status.sh
# -> T1: as 3 apps aplicam a mudança na hora (watch)

# E6: isolar o LÍDER da rede interna (troque etcd1 pelo líder atual)
docker network disconnect t5-etcd-enfasec_interna etcd1
./scripts/status.sh            # dois "líderes" por ~1 s, termos diferentes; depois só um
docker exec etcd1 etcdctl --endpoints=http://127.0.0.1:2379 --command-timeout=3s put /x 1        # falha
docker exec etcd1 etcdctl --endpoints=http://127.0.0.1:2379 get /config/modo --consistency=s     # valor antigo
# -> T2: o escritor continua sem falhas (o cliente trocou de membro)

# E7: reconectar e ver o antigo líder voltar como seguidor, com termo novo
docker network connect --ip 172.30.0.31 t5-etcd-enfasec_interna etcd1
./scripts/status.sh
```

Falas-chave: "o lado minoritário prefere ficar indisponível a responder errado: CP";
"o termo é a época de liderança; a revisão é a ordem total das escritas".

### Bloco 4 – ênfase C (T4)

```bash
REMOVER=lider MOVER_LIDER=1 ./scripts/e9_reconfig.sh
# ENTER entre as etapas, narrando: learner -> catch-up -> promote -> move-leader -> remove
```

Mostrar: o learner recusa leitura linearizável e aceita a serializável; com 4 votantes o quórum é 3
(não tolera mais falhas que 3 membros: S2); no T2 o escritor não registra falhas nem pausas. Fechar com os
números do relatório (E9.2): remover o líder direto = 1–3 s sem escritas; com `move-leader` antes = 92 ms.

## Perguntas prováveis na arguição (Q1–Q14, S1–S4)

- Por que 3 e não 4 membros? (S2: f = ⌊(N−1)/2⌋)
- O que acontece se o líder for isolado? (E6/Q11)
- Diferença entre leitura linearizável e serializável? Como o ReadIndex funciona? (S3)
- Por que o timeout de eleição é aleatório? (S1)
- Como uma trava com lease falha numa pausa longa? O que é fencing token? (S4)
- Por que `docker stop` elegeu em 16 ms e `docker kill` em 1,2 s? (E5: transferência de liderança)
- Como a app não perde mudanças se o membro cair? (E9: retomada pela revisão)
