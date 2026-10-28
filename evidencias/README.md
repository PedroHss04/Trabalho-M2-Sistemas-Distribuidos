# Teste prático: montagem e validação do ambiente

Evidências de que o laboratório (Tema 5, etcd/Raft, Ênfase C) **sobe do zero e funciona**, usando
apenas contêineres leves. Cada item tem o **comando**, o **resultado esperado**, o **resultado
observado** e a **evidência**: um print da janela do terminal (`prints/`) e a transcrição completa,
com comando e saída (`comandos/`).

Coleta feita em 21/10/2026.

## Ambiente usado

| Item | Valor |
|---|---|
| Máquina | AMD Ryzen 7 5700X (8 núcleos / 16 threads), 15,9 GB de RAM |
| Sistema | Windows 11 Home 10.0.26300 |
| Docker | Docker Desktop 4.94, Engine 29.8.2, Compose v5.5.1, backend WSL 2 3.0.3 (kernel 6.18.54.1) |
| Imagens | `quay.io/coreos/etcd:v3.5.17` (85,4 MB) e `python:3.12-alpine` (83,6 MB) |
| Shell | Git Bash (GNU bash 5.3), Git 2.54 |

**Sem máquinas virtuais.** A equipe não criou nem configurou nenhuma VM (VirtualBox, VMware, Hyper-V
etc.). Tudo roda em contêineres do Docker Desktop, que no Windows usa o backend WSL 2, a forma de
instalação indicada no roteiro (seção 4.3). São 7 contêineres: 3 membros etcd, 3 aplicações e
1 cliente de medição.

## Validações

| # | O que valida | Comando principal | Esperado | Observado | Evidência |
|---|---|---|---|---|---|
| 1 | Ferramentas instaladas | `docker version`, `docker compose version`, `docker info` | Docker e Compose funcionando, engine Linux | Docker 29.8.2, Compose v5.5.1, engine Linux/amd64 sobre WSL 2 | [print](prints/01_ferramentas.png) · [saída](comandos/01_ferramentas.txt) |
| 2 | Subida do zero | `./scripts/reset.sh` (`compose down` + `up --wait`) | 7 contêineres criados; 3 membros *healthy*; líder eleito | 2 redes e 7 contêineres criados, todos *Healthy*; líder etcd3 no termo 2; pronto em **14,8 s** | [print](prints/02_subida_do_zero.png) · [saída](comandos/02_subida.txt) |
| 3 | Contêineres e imagens | `docker compose ps`, `docker image ls` | Todos *Up*; membros etcd *healthy*; imagens leves | 7 *Up*, 3 *healthy*; imagens de 85,4 MB e 83,6 MB | [print](prints/03_conteineres.png) · [saída](comandos/03_conteineres.txt) |
| 4 | Cluster com ≥ 3 nós | `etcdctl member list / endpoint status / endpoint health` | 3 membros `started`, 1 líder, mesmo termo e índice, saúde `true` | 3 membros; etcd3 líder; termo 2 e índice 82 nos três; `health` `true` em 2–3 ms | [print](prints/04_cluster_lider_e_saude.png) · [saída](comandos/04_cluster.txt) |
| 5 | Redes e endereços | `docker inspect` (IPs por rede) | Membros nas 2 redes; apps e cliente só na rede de clientes | etcd1–3 em `interna` (172.30.0.31–33) e `clientes` (172.31.0.31–33); apps .101–.103; cliente .100 | [print](prints/05_redes_e_enderecos.png) · [saída](comandos/05_redes.txt) |
| 6 | Limites de memória respeitados (R1) | `docker inspect` (limite) e `docker stats` | Uso bem abaixo de 256 MB (etcd) e 64 MB (demais) | etcd 14–29 MiB de 256 MiB; apps 13–15 MiB de 64 MiB; cliente 2 MiB | [print](prints/06_memoria_limite_x_uso.png) · [saída](comandos/06_memoria.txt) |
| 7 | Funcionamento | `put` no etcd1, `get` no etcd2 e no etcd3, logs das apps | Valor lido nos outros membros; as 3 apps aplicam via *watch* | Valor visto no etcd2 (linearizável) e no etcd3 (serializável); as 3 apps aplicaram a rev 10 no mesmo segundo e gravaram ACK | [print](prints/07_teste_funcional.png) · [saída](comandos/07_funcional.txt) |
| 8 | Tolerância a 1 falha | `docker stop etcd3`, `put`/`get`, `docker start etcd3` | Com 2 de 3 o cluster aceita escrita; o membro volta e alcança os outros | O etcd3 parado era o líder; o etcd2 assumiu no termo 3; escrita e leitura OK com 2 de 3; ao voltar, o etcd3 alcança o mesmo índice (157) | [print](prints/08_tolerancia_1_falha.png) · [saída](comandos/08_tolerancia.txt) |

No print 8, a mensagem `Failed to get the status of endpoint http://172.31.0.33:2379 (context
deadline exceeded)` é esperada: é o membro que acabou de ser parado.

Os prints e as transcrições vêm de duas execuções diferentes, na mesma sessão. Por isso os valores
que dependem do momento, como o líder, o índice e o uso de memória, diferem um pouco entre eles.
Nas transcrições, `ctl etcdN ...` é a função de `scripts/lib.sh` que executa
`docker exec etcdN etcdctl --endpoints=http://127.0.0.1:2379 ...`, e `sonda` executa
`docker exec cliente python /scripts/sonda.py`.

## Como reproduzir

Requisitos: Docker Desktop (com WSL 2 no Windows) e Git. Na pasta do repositório, em Git Bash, WSL
ou Linux:

```bash
./evidencias/coletar.sh          # recria o ambiente do zero e regrava evidencias/comandos/*.txt
```

Os prints foram tirados com `evidencias/capturar_print.ps1`. Ele abre uma janela nova do Windows
Terminal, roda os comandos nela e captura **só essa janela**:

```powershell
powershell -ExecutionPolicy Bypass -File evidencias\capturar_print.ps1 `
  -Saida evidencias\prints\04_cluster_lider_e_saude.png -Espera 7 `
  -Comandos "docker exec etcd1 etcdctl --endpoints=http://172.31.0.31:2379 endpoint status -w table"
```

As evidências dos experimentos E1–E9 (falha, partição, recuperação, reconfiguração etc.) estão em
[`../resultados/`](../resultados/) e são analisadas no relatório.
