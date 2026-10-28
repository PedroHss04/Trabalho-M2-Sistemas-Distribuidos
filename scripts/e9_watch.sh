#!/usr/bin/env bash
# E9 (parte 1) - Ênfase C: configuração dinâmica via watch.
#  1. Tempo de propagação put -> as 3 apps aplicaram (N repetições, CSV).
#  2. O watch sobrevive à queda do membro em que as apps estão conectadas (retomada por revisão).
#  3. Compactação: app desconectado perde a janela de histórico; ao voltar, o watch é cancelado
#     (revisão compactada) e o app relê o estado completo antes de continuar.
#  4. Partição: o membro que serve o watch das apps é isolado da rede interna (perde o líder);
#     as apps devem trocar de membro e aplicar a mudança feita pela maioria.
. "$(dirname "$0")/lib.sh"
registrar e9_watch
N=${1:-20}
trap 'docker network connect --ip 172.31.0.101 "$REDE_CLIENTES" app1 2>/dev/null || true; restaurar' EXIT
hora() { date +%H:%M:%S; }
logs_apps() { docker compose logs --no-log-prefix --since "$1" app1 app2 app3 2>/dev/null | sort; }
# membro em que a app ($1, padrão app1) abriu o último watch (etcdN)
membro_das_apps() {
  local ip
  ip=$(docker compose logs "${1:-app1}" 2>/dev/null | grep -o 'watch aberto em http://[0-9.]*' | tail -1 | grep -o '172[0-9.]*')
  echo "etcd${ip##*.3}"
}

etapa "1. Propagação put -> apps aplicaram ($N repetições)"
docker exec cliente python /scripts/medir_watch.py "$N" | grep -v '^rep '

etapa "2. Queda do membro que serve o watch das apps"
M=$(membro_das_apps)
echo "as apps estão com o watch aberto em $(ep_cliente "$M") ($M); derrubando $M às $(hora)"
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

etapa "4. Partição: membro que serve o watch das apps isolado da rede interna"
M=$(membro_das_apps)
ERA_LIDER=nao; [ "$(lider)" = "$M" ] && ERA_LIDER=sim
echo "$(hora) watch das apps: app1 em $M, app2 em $(membro_das_apps app2), app3 em $(membro_das_apps app3)"
echo "$(hora) isolando $M (era líder: $ERA_LIDER) da rede interna"
docker network disconnect "$REDE_INTERNA" "$M"
sleep 3
OUTRO=$(for m in $MEMBROS; do [ "$m" != "$M" ] && echo "$m" && break; done)
ctl "$OUTRO" put /config/modo durante-a-particao >/dev/null
T0=$(agora)
echo "$(hora) put /config/modo=durante-a-particao pela maioria (via $OUTRO); aguardando as apps (até 30 s)"
declare -A APLICOU=()
for _ in $(seq 1 30); do
  for a in app1 app2 app3; do
    [ -z "${APLICOU[$a]:-}" ] && docker compose logs --since 40s "$a" 2>/dev/null \
      | grep -q 'APLICADA /config/modo = durante-a-particao' && APLICOU[$a]=$(dif "$T0" "$(agora)")
  done
  [ "${#APLICOU[@]}" = 3 ] && break
  sleep 1
done
for a in app1 app2 app3; do
  if [ -n "${APLICOU[$a]:-}" ]; then echo "$a: OK (aplicou em ${APLICOU[$a]} s)"
  else echo "$a: FALHA (não aplicou em 30 s)"; fi
done
logs_apps 40s
docker network connect --ip "$(ip_peer "$M")" "$REDE_INTERNA" "$M"
sonda esperar-saudavel 60
