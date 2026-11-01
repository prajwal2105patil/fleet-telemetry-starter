#!/usr/bin/env bash
# Pre-work check. Run from the repo root:  ./scripts/check_env.sh
# Send a screenshot of the final "ALL GOOD" (or the failures) to your trainer.
set -u
ok=0; fail=0
pass() { echo "  [OK]   $1"; ok=$((ok+1)); }
bad()  { echo "  [FAIL] $1"; fail=$((fail+1)); }

echo "== Tools"
command -v docker >/dev/null && pass "docker $(docker --version | awk '{print $3}' | tr -d ,)" || bad "docker not installed (install Docker Desktop)"
docker compose version >/dev/null 2>&1 && pass "docker compose" || bad "docker compose plugin missing"
command -v git >/dev/null && pass "git" || bad "git not installed"
if ! command -v python3 >/dev/null; then
  bad "python3 missing (needed for the simulator)"
else
  pv=$(python3 -c 'import sys;print(sys.version.split()[0])')
  if python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)'; then
    pass "python3 $pv"
  else
    bad "python3 $pv is too old - Python 3.10+ is required (macOS ships 3.9; e.g. brew install python@3.12)"
  fi
fi
if [ -d java ] && [ "${STACK:-}" != "python" ]; then
  jv=$(java -version 2>&1 | awk -F'"' '/version/ {print $2; exit}' | cut -d. -f1)
  [ -n "$jv" ] && [ "$jv" -ge 17 ] 2>/dev/null && pass "java $jv" \
    || bad "Java 17+ not found (Java track only - on the Python track run: STACK=python ./scripts/check_env.sh)"
fi

echo "== Containers"
docker compose ps --format '{{.Service}} {{.State}} {{.Health}}' 2>/dev/null | while read -r s st h; do echo "         $s: $st $h"; done
for s in kafka postgres; do
  h=$(docker inspect -f '{{.State.Health.Status}}' "fleet-$s" 2>/dev/null || echo missing)
  [ "$h" = "healthy" ] && pass "$s healthy" || bad "$s is '$h' — run: docker compose up -d   (then wait ~30s)"
done

echo "== Kafka topics"
topics=$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null)
for t in telemetry.raw telemetry.dlq alerts.out; do
  echo "$topics" | grep -qx "$t" && pass "topic $t" || bad "topic $t missing — run: docker compose up kafka-init"
done

echo "== Database"
n=$(docker compose exec -T postgres psql -U fleet -d fleet -tAc "select count(*) from telemetry" 2>/dev/null | tr -d '[:space:]')
[ -n "$n" ] && [ "$n" -gt 1000 ] 2>/dev/null && pass "seed data loaded ($n telemetry rows)" || bad "seed data missing — run: docker compose down -v && docker compose up -d"

echo "== Your app (optional until Day 2)"
if curl -fsS http://localhost:8080/health >/dev/null 2>&1; then pass "app /health on :8080"; else echo "  [--]   app not running on :8080 (start it to complete the check)"; fi

echo
[ $fail -eq 0 ] && echo "ALL GOOD ($ok checks passed)" || { echo "$fail check(s) failed — fix them before Day 2"; exit 1; }
