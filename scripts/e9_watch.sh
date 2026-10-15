#!/usr/bin/env bash
# E9 (parte 1) - Ênfase C: configuração dinâmica via watch.
#  1. Tempo de propagação put -> as 3 apps aplicaram (N repetições, CSV).
#  2. O watch sobrevive à queda do membro em que as apps estão conectadas (retomada por revisão).
#  3. Compactação: app desconectado perde a janela de histórico; ao voltar, o watch é cancelado
#     (revisão compactada) e o app relê o estado completo antes de continuar.
. "$(dirname "$0")/lib.sh"
registrar e9_watch
N=${1:-20}
trap 'docker network connect --ip 172.31.0.101 "$REDE_CLIENTES" app1 2>/dev/null || true; restaurar' EXIT
hora() { date +%H:%M:%S; }
logs_apps() { docker compose logs --no-log-prefix --since "$1" app1 app2 app3 2>/dev/null | sort; }

etapa "1. Propagação put -> apps aplicaram ($N repetições)"
docker exec cliente python /scripts/medir_watch.py "$N" | grep -v '^rep '

etapa "2. Queda do membro que serve o watch das apps"
ALVO=$(docker compose logs app1 2>/dev/null | grep -o 'watch aberto em http://[0-9.]*' | tail -1 | grep -o '172[0-9.]*')
M="etcd${ALVO##*.3}"
echo "as apps estão com o watch aberto em $ALVO ($M); derrubando $M às $(hora)"
docker stop "$M" >/dev/null
OUTRO=$(for m in $MEMBROS; do [ "$m" != "$M" ] && echo "$m" && break; done)
for v in a b c; do ctl "$OUTRO" put /config/modo "durante-a-queda-$v" >/dev/null; done
echo "3 puts feitos via $OUTRO; aguardando as apps (detecção pelo fim da conexão / progress notify)..."
sleep 8
logs_apps 15s
docker start "$M" >/dev/null; sonda esperar-saudavel 60 >/dev/null

etapa "3. Compactação com o app1 desconectado da rede de clientes"
docker network disconnect "$REDE_CLIENTES" app1
echo "$(hora) app1 desconectado; 5 mudanças de configuração e compactação do histórico"
for i in 1 2 3 4 5; do ctl etcd1 put /config/versao "$i" >/dev/null; done
REV=$(ctl etcd1 put /config/modo pos-compactacao -w json | grep -o '"revision":[0-9]*' | cut -d: -f2)
ctl etcd1 compact "$REV"
echo "$(hora) mantendo o app1 fora por 20 s (mais que o timeout de leitura do watch, 15 s)"
sleep 20
docker network connect --ip 172.31.0.101 "$REDE_CLIENTES" app1
echo "$(hora) app1 reconectado; aguardando o watch dele reabrir"
for _ in $(seq 1 40); do
  docker compose logs --since 40s app1 2>/dev/null | grep -q 'estado relido' && break; sleep 1
done
docker compose logs --no-log-prefix --since 40s app1
echo
echo ">> app2/app3 (sempre conectados) receberam cada evento; o app1 recebeu o estado final relido."
