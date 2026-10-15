# Funções comuns aos scripts de experimento (carregar com: . "$(dirname "$0")/lib.sh").
set -euo pipefail
# Git Bash (Windows) converteria "/config/x" em "C:/Program Files/Git/config/x"
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mkdir -p resultados

REDE_INTERNA=t5-etcd-enfasec_interna
EP=http://172.31.0.31:2379,http://172.31.0.32:2379,http://172.31.0.33:2379
MEMBROS="etcd1 etcd2 etcd3"

ip_peer()    { echo "172.30.0.3${1#etcd}"; }
ep_cliente() { echo "http://172.31.0.3${1#etcd}:2379"; }

# etcdctl dentro de um membro, falando SÓ com ele mesmo (endpoint único)
ctl() { local m=$1; shift; docker exec "$m" etcdctl --endpoints=http://127.0.0.1:2379 "$@"; }
# etcdctl com a lista completa (o cliente troca de membro sozinho se um cair)
ctl_todos() { local m=$1; shift; docker exec "$m" etcdctl --endpoints="$EP" "$@"; }
# sonda de medição (Python, dentro do contêiner cliente: mesmo relógio, sem overhead do docker exec)
sonda() { docker exec cliente python /scripts/sonda.py "$@"; }

# sonda em background, com a saída num arquivo; esperar_pronto bloqueia até ela começar a observar
# (use "wait $SONDA_PID", nunca "wait" sozinho: no bash 5.1+ ele esperaria também o tee do log)
sonda_bg() { local arq=$1; shift; docker exec cliente python -u /scripts/sonda.py "$@" >"$arq" 2>&1 & SONDA_PID=$!; }
esperar_pronto() { for _ in $(seq 1 100); do grep -q pronto "$1" 2>/dev/null && return; sleep 0.1; done; cat "$1"; return 1; }

REDE_CLIENTES=t5-etcd-enfasec_clientes
IMAGEM_ETCD=quay.io/coreos/etcd:v3.5.17
# etcdctl num contêiner descartável na rede de clientes (watch, lock e elect de longa duração).
# Parar ESSE contêiner simula a falha do cliente sem derrubar nenhum membro do cluster.
avulso() {
  local n=$1; shift
  docker rm -f "$n" >/dev/null 2>&1 || true
  docker run -d --name "$n" --network "$REDE_CLIENTES" --memory 32m "$IMAGEM_ETCD" \
    etcdctl --endpoints="$EP" "$@" >/dev/null
}
limpar_avulsos() { docker rm -f "$@" >/dev/null 2>&1 || true; }

rodando() { [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = true ]; }
algum_vivo() { for m in $MEMBROS etcd4; do rodando "$m" && { echo "$m"; return; }; done; }
lider() { sonda lider; }   # nome do líder atual (etcd1..etcd4)

agora() { date +%s.%N; }
dif()   { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.3f", b-a}'; }
etapa() { echo; echo "=================== $* ==================="; }
pausa() { [ "${AUTO:-0}" = 1 ] || read -rp ">> ENTER para continuar..." _; }

# grava toda a saída do script em resultados/<nome>_<data>.log (além da tela)
registrar() {
  LOG="resultados/$1_$(date +%Y%m%d_%H%M%S).log"
  exec > >(tee -a "$LOG") 2>&1
  echo "# $1 — $(date '+%Y-%m-%d %H:%M:%S') — log em $LOG"
}

# média, desvio padrão amostral, mín e máx de uma coluna numérica (stdin, um valor por linha)
estatistica() {
  awk '{x[NR]=$1; s+=$1} END{
    if (NR==0) {print "n=0"; exit}
    m=s/NR; for(i=1;i<=NR;i++) q+=(x[i]-m)^2; dp=(NR>1)?sqrt(q/(NR-1)):0
    mn=x[1]; mx=x[1]; for(i=1;i<=NR;i++){if(x[i]<mn)mn=x[i]; if(x[i]>mx)mx=x[i]}
    printf "n=%d  média=%.3f  desvio=%.3f  mín=%.3f  máx=%.3f\n", NR, m, dp, mn, mx}'
}

# volta ao cluster de 3 membros saudável (sem recriar dados), religando redes e contêineres
restaurar() {
  for m in $MEMBROS; do
    rodando "$m" || docker start "$m" >/dev/null
    docker network inspect "$REDE_INTERNA" -f '{{range .Containers}}{{.Name}} {{end}}' | grep -qw "$m" \
      || docker network connect --ip "$(ip_peer "$m")" "$REDE_INTERNA" "$m"
  done
  sonda esperar-saudavel 60 >/dev/null
}
