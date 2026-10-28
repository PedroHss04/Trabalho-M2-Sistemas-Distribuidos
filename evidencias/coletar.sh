#!/usr/bin/env bash
# Teste prático: monta o ambiente do zero e valida, gravando cada comando e sua saída em
# evidencias/comandos/NN_<nome>.txt (transcrição "$ comando" + saída real).
#   ./evidencias/coletar.sh
. "$(dirname "$0")/../scripts/lib.sh"
set +e   # queremos registrar também saídas de erro, sem abortar
DIR=evidencias/comandos
mkdir -p "$DIR"

# abre um arquivo de evidência: tudo o que for impresso até o próximo "secao" vai para ele
secao() { exec >"$DIR/$1.txt" 2>&1; echo "# $2"; echo "# coletado em $(date '+%Y-%m-%d %H:%M:%S %z')"; echo; }
# imprime o comando como num terminal e executa
c() { echo "\$ $*"; eval "$@"; echo; }

secao 01_ferramentas "Ferramentas instaladas (Docker Desktop + WSL 2; nenhuma VM criada pela equipe)"
c docker version --format "'Cliente {{.Client.Version}} / Servidor {{.Server.Version}} ({{.Server.Os}}/{{.Server.Arch}})'"
c docker compose version
c docker info --format "'SO do engine: {{.OperatingSystem}} | kernel: {{.KernelVersion}} | CPUs: {{.NCPU}} | memória: {{.MemTotal}} bytes | cgroup: {{.CgroupVersion}}'"
c git --version
c bash --version \| head -1

secao 02_subida "Subida do zero: docker compose down + up --wait (healthcheck) + configuração inicial"
T0=$(agora)
c ./scripts/reset.sh
echo ">> ambiente pronto em $(dif "$T0" "$(agora)") s"

secao 03_conteineres "Contêineres em execução, imagens usadas e tamanho"
c docker compose ps --format "'table {{.Name}}\t{{.Image}}\t{{.Status}}'"
c docker image ls --format "'table {{.Repository}}:{{.Tag}}\t{{.Size}}'" quay.io/coreos/etcd:v3.5.17
c docker image ls --format "'table {{.Repository}}:{{.Tag}}\t{{.Size}}'" python:3.12-alpine

secao 04_cluster "Cluster etcd: membros, líder, termo, índice do Raft e saúde"
c ctl etcd1 member list -w table
c ctl_todos etcd1 endpoint status -w table
c ctl_todos etcd1 endpoint health -w table

secao 05_redes "Redes e endereços (interna = Raft :2380, clientes = API :2379)"
c docker network ls --filter name=t5-etcd-enfasec
for cn in etcd1 etcd2 etcd3 app1 app2 app3 cliente; do
  c docker inspect -f "'{{.Name}} {{range \$n, \$e := .NetworkSettings.Networks}} {{\$n}}={{\$e.IPAddress}}{{end}}'" "$cn"
done

secao 06_memoria "Limites de memória declarados x uso real (R1)"
for cn in etcd1 etcd2 etcd3 app1 app2 app3 cliente; do
  c docker inspect -f "'{{.Name}} limite={{.HostConfig.Memory}} bytes'" "$cn"
done
c docker stats --no-stream --format "'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}'"

secao 07_funcional "Validação funcional: escrita num membro, leitura nos outros, apps reagindo via watch"
V="teste-pratico-$(date +%H%M%S)"
c ctl etcd1 put /config/modo "$V"
c ctl etcd2 get /config/modo
c ctl etcd3 get /config/modo --consistency=s
sleep 1
c docker compose logs --no-log-prefix --since 10s app1 app2 app3 \| sort
c ctl etcd1 get /acks/ --prefix

secao 08_tolerancia "Validação rápida de tolerância a falha: 1 membro parado, o cluster continua"
c docker stop etcd3
c ctl etcd1 --command-timeout=3s put /validacao ok-com-2-de-3
c ctl etcd2 get /validacao
c docker start etcd3
c sonda esperar-saudavel 60
c ctl_todos etcd1 endpoint status -w table
