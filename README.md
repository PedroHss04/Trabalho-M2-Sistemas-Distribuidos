# Trabalho M2 – Sistemas Distribuídos – Tema 5 (etcd / Raft) – Ênfase C

**Configuração dinâmica e membros:** aplicações que reagem a mudanças de configuração via `watch`
e reconfiguração do cluster (learner → promote → remove) sem perda de disponibilidade.

- **Roteiro da demonstração ao vivo:** [SEMINARIO.md](SEMINARIO.md)
- **Evidências:** [resultados/](resultados/) (logs completos e CSVs de cada experimento)

## Topologia

| Contêiner | Papel | Rede interna (Raft, 2380) | Rede clientes (API, 2379) | Memória |
|---|---|---|---|---|
| etcd1 | membro etcd | 172.30.0.31 | 172.31.0.31 | 256 MB |
| etcd2 | membro etcd | 172.30.0.32 | 172.31.0.32 | 256 MB |
| etcd3 | membro etcd | 172.30.0.33 | 172.31.0.33 | 256 MB |
| etcd4 | 4º membro (profile `extra`, entra como learner no E9) | 172.30.0.34 | 172.31.0.34 | 256 MB |
| app1..3 | aplicações que observam `/config/` via watch | – | 172.31.0.101–103 | 64 MB |
| cliente | roda os scripts Python de medição (sonda) | – | 172.31.0.100 | 64 MB |

Imagens: `quay.io/coreos/etcd:v3.5.17` e `python:3.12-alpine`. As apps e a sonda usam só a
biblioteca padrão do Python (API v3 do etcd via gateway HTTP/JSON), então não há `pip install`.

## Subir do zero

Requisitos: Docker (Docker Desktop com WSL 2 no Windows) e Git. Os scripts são `bash`:
rodam no terminal do WSL, no Linux/macOS ou no **Git Bash** do Windows (os scripts já
exportam `MSYS_NO_PATHCONV=1`, que impede o Git Bash de trocar `/config/x` por um caminho
do Windows).

```bash
git clone https://github.com/PedroHss04/Trabalho-M2-Sistemas-Distribuidos.git
cd Trabalho-M2-Sistemas-Distribuidos
./scripts/reset.sh        # recria tudo, espera o healthcheck dos 3 membros e grava a config inicial
./scripts/status.sh       # membros, líder, termo, índice do Raft e saúde
docker compose logs -f app1 app2 app3   # acompanhar as aplicações
```

O primeiro `reset.sh` baixa as imagens (~85 MB). `docker compose down` apaga os dados; o
`reset.sh` faz isso de propósito para cada experimento começar de um estado conhecido.

## Experimentos (cada script grava o log completo em `resultados/<exp>_<data>.log`)

| Exp. | Script | O que faz | Duração |
|---|---|---|---|
| E1 | `./scripts/e1_topologia.sh` | member list, endpoint status/health, IPs por rede, memória x limite | 5 s |
| E2 | `./scripts/e2_caminho.sh` | put num seguidor (-w json), RAFT INDEX nos 3, put direto x via seguidor (100x) | 10 s |
| E3 | `./scripts/e3_replicacao.sh` | escreve no etcd1, lê no etcd2, watch no etcd3; propagação (20x); `check perf` | 90 s |
| E4 | `./scripts/e4_consistencia.sh` | linearizável x serializável (200x); seguidor isolado devolve valor antigo/erro | 20 s |
| E5 | `./scripts/e5_falha.sh` | derruba o líder 5x com `kill` e 5x com `stop`; quadro 3/2/1 membros ativos | 2 min |
| E6 | `./scripts/e6_particao.sh` | isola o líder da rede interna; os dois lados da partição; conclusão C x A | 30 s |
| E7 | `./scripts/e7_recuperacao.sh` | reconexão do líder isolado (5x); recuperação por snapshot x por log | 4 min |
| E8 | `./scripts/e8_coordenacao.sh` | termo x revisão; `lock` e `elect` com falha do detentor (5x cada) | 8 min |
| E9.1 | `./scripts/e9_watch.sh` | propagação put → apps (20x); watch sobrevive à queda e à partição do membro; compactação | 2 min |
| E9.2 | `./scripts/e9_reconfig.sh` | learner → catch-up → promote → remove (com pausas, para o vídeo) | 1 min |
| E9.2 | `./scripts/e9_repeticoes.sh 5` | 5 rodadas completas com escritor contínuo (R7) | 6 min |

`REPS=N` muda o número de repetições do E5, E7 e E8. `AUTO=1` tira as pausas do `e9_reconfig.sh`.
`REMOVER=lider` faz o E9 remover o líder (força eleição durante a reconfiguração);
`REMOVER=lider MOVER_LIDER=1` transfere a liderança com `etcdctl move-leader` antes de remover.
Os três cenários usados no relatório:

```bash
./scripts/e9_repeticoes.sh 5                               # remove um seguidor
REMOVER=lider ./scripts/e9_repeticoes.sh 5                 # remove o líder
REMOVER=lider MOVER_LIDER=1 ./scripts/e9_repeticoes.sh 5   # move-leader e depois remove
```

## Estrutura

```
docker-compose.yml       cluster (healthcheck, limites de memória) + apps + cliente
app/etcdhttp.py          cliente mínimo do etcd (put/get/watch com failover e retomada por revisão)
app/app_watch.py         aplicação que aplica a configuração a cada mudança e grava ACK em /acks/<app>
scripts/lib.sh           funções comuns (etcdctl por membro, sonda, estatística, restaurar cluster)
scripts/sonda.py         medições dentro do contêiner cliente (eleição, catch-up, propagação, leituras)
scripts/e1..e9_*.sh      um script por experimento (ver tabela acima)
scripts/medir_watch.py   E9.1: atraso put -> app aplicou
scripts/escritor.py      E9.2: escritas a cada 50 ms (prova de disponibilidade)
scripts/carga.py         gera milhares de chaves (catch-up mensurável no E7 e no E9)
scripts/marcos_log.py    E7.2: linha do tempo da recuperação a partir do log JSON do etcd
scripts/reset.sh         recria o laboratório (SNAPSHOT_COUNT=1000 para o E7.2)
scripts/status.sh        estado do cluster
resultados/              logs e CSVs gerados (evidências do relatório)
```

## Por que uma "sonda" dentro do contêiner?

No Windows cada `docker exec` custa ~200 ms, mais que uma eleição por desligamento gracioso
(~15 ms). A `sonda.py` roda dentro do contêiner `cliente`, consulta `/v3/maintenance/status`
de cada membro em threads paralelas a cada ~10 ms e mede tudo com um único relógio. Membros
mortos não atrasam a consulta dos vivos.

## Referência rápida (comandos manuais)

```bash
EP=172.31.0.31:2379,172.31.0.32:2379,172.31.0.33:2379
docker exec etcd1 etcdctl --endpoints=$EP endpoint status -w table          # líder, termo, índice
docker exec etcd2 etcdctl --endpoints=172.31.0.32:2379 put /x 1 -w json     # put num seguidor
docker exec etcd3 etcdctl --endpoints=172.31.0.33:2379 watch /x             # watch noutro membro
docker exec etcd1 etcdctl get /x --consistency=s                            # leitura serializável
docker network disconnect t5-etcd-enfasec_interna etcd1                     # isolar etcd1 (Raft)
docker network connect --ip 172.30.0.31 t5-etcd-enfasec_interna etcd1       # reconectar
docker exec etcd1 etcdctl lock trava                                        # exclusão mútua
docker exec etcd2 etcdctl elect eleicao candidatoA                          # eleição
```

Armadilhas: use **endpoint único** para observar um membro específico (com a lista completa, o
cliente troca de membro sozinho). `docker stop` faz o etcd transferir a liderança antes de sair
(eleição de ~15 ms); para simular uma queda real use `docker kill`.

## Limitações

- Todos os contêineres compartilham o relógio da VM do Docker; as medições usam esse relógio comum,
  o que não seria possível em máquinas separadas sem sincronização.
- Rede virtual local sem latência real; disco único compartilhado pelos 3 membros.
- Sem TLS e sem autenticação (apenas laboratório).
