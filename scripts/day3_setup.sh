#!/usr/bin/env bash
# Day 3 setup. Run from the repo root on the morning of Day 3:
#
#   ./scripts/day3_setup.sh            # adds the Day 3 data + tickets; keeps your Day 2 data; safe to run again
#   ./scripts/day3_setup.sh --reset    # ONLY if your database is broken: deletes everything (incl. Day 2 data),
#                                      # re-creates the database from the seed, then applies the Day 3 setup
set -euo pipefail

cd "$(dirname "$0")/.."
SQL=db/day3/day3_setup.sql
psql() { docker compose exec -T postgres psql -U fleet -d fleet "$@"; }

wait_for_seed() {
  echo "Waiting for Postgres and the seed data..."
  for _ in $(seq 1 90); do
    h=$(docker inspect -f '{{.State.Health.Status}}' fleet-postgres 2>/dev/null || echo missing)
    if [ "$h" = "healthy" ]; then
      n=$(psql -tAc "select count(*) from telemetry" 2>/dev/null | tr -d '[:space:]' || true)
      [ -n "$n" ] && [ "$n" -gt 1000 ] 2>/dev/null && { echo "Database ready ($n telemetry rows)."; return 0; }
    fi
    sleep 2
  done
  echo "Postgres did not become ready in 3 minutes. Check: docker compose ps / docker compose logs postgres" >&2
  exit 1
}

case "${1:-}" in
  --reset)
    echo "Resetting: this deletes ALL data, including your Day 2 data."
    docker compose down -v
    docker compose up -d
    wait_for_seed
    ;;
  "")
    wait_for_seed
    ;;
  *)
    echo "Usage: $0 [--reset]" >&2
    exit 2
    ;;
esac

psql -q -v ON_ERROR_STOP=1 -f - < "$SQL"

echo
echo "Tickets:   docker compose exec postgres psql -U fleet -d fleet -c 'select id, title from ops_tickets order by id'"
echo "Files:     data/day3/transfer_cityride_to_autofin.csv (FIX-3)"
echo "           data/day3/app.log (script task 2)"
