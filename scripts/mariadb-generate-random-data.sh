#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

usage() {
  cat <<USAGE
Usage: $(basename "$0") --container NAME [options]

Generate high-entropy MariaDB test data by truncating and refilling a table
until it reaches a target size.

Required:
  --container NAME           Docker container name running MariaDB

Options:
  --target-gib N             Target table size in GiB (default: 10)
  --db NAME                  Database name (default: vger_maria_test)
  --table NAME               Table name (default: large_payload)
  --root-user USER           MariaDB admin user (default: root)
  --root-password PASS       MariaDB admin password (default: testpass)
  --socket PATH              MariaDB socket path in container
                             (default: /run/mysqld/mysqld.sock)
  --rows-per-batch N         Rows inserted per batch (default: 2048)
  --raw-bytes-per-row N      Random bytes generated per row before base64
                             (default: 49152)
  --payload-chars N          Characters per inserted row payload
                             (default: 65536)
  --progress-every N         Print size progress every N batches (default: 4)
  --max-batches N            Safety cap on batch loops (default: 5000)
  --no-truncate              Keep existing rows and append only
  --help                     Show this help

Examples:
  $(basename "$0") --container vger-maria --target-gib 10
  $(basename "$0") --container vger-maria --target-gib 30 --rows-per-batch 1024
USAGE
}

CONTAINER=""
TARGET_GIB=10
DB_NAME="vger_maria_test"
TABLE_NAME="large_payload"
ROOT_USER="root"
ROOT_PASSWORD="testpass"
SOCKET_PATH="/run/mysqld/mysqld.sock"
ROWS_PER_BATCH=2048
RAW_BYTES_PER_ROW=49152
PAYLOAD_CHARS=65536
PROGRESS_EVERY=4
MAX_BATCHES=5000
TRUNCATE_TABLE=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --container) CONTAINER="${2:-}"; shift 2 ;;
    --target-gib) TARGET_GIB="${2:-}"; shift 2 ;;
    --db) DB_NAME="${2:-}"; shift 2 ;;
    --table) TABLE_NAME="${2:-}"; shift 2 ;;
    --root-user) ROOT_USER="${2:-}"; shift 2 ;;
    --root-password) ROOT_PASSWORD="${2:-}"; shift 2 ;;
    --socket) SOCKET_PATH="${2:-}"; shift 2 ;;
    --rows-per-batch) ROWS_PER_BATCH="${2:-}"; shift 2 ;;
    --raw-bytes-per-row) RAW_BYTES_PER_ROW="${2:-}"; shift 2 ;;
    --payload-chars) PAYLOAD_CHARS="${2:-}"; shift 2 ;;
    --progress-every) PROGRESS_EVERY="${2:-}"; shift 2 ;;
    --max-batches) MAX_BATCHES="${2:-}"; shift 2 ;;
    --no-truncate) TRUNCATE_TABLE=0; shift ;;
    --help|-h) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

[[ -n "$CONTAINER" ]] || die "--container is required"
[[ "$CONTAINER" =~ ^[a-zA-Z0-9_.-]+$ ]] || die "invalid --container"
[[ "$DB_NAME" =~ ^[a-zA-Z0-9_]+$ ]] || die "invalid --db"
[[ "$TABLE_NAME" =~ ^[a-zA-Z0-9_]+$ ]] || die "invalid --table"
[[ "$TARGET_GIB" =~ ^[0-9]+$ ]] || die "--target-gib must be a non-negative integer"
[[ "$ROWS_PER_BATCH" =~ ^[0-9]+$ && "$ROWS_PER_BATCH" -gt 0 ]] || die "--rows-per-batch must be > 0"
[[ "$RAW_BYTES_PER_ROW" =~ ^[0-9]+$ && "$RAW_BYTES_PER_ROW" -gt 0 ]] || die "--raw-bytes-per-row must be > 0"
[[ "$PAYLOAD_CHARS" =~ ^[0-9]+$ && "$PAYLOAD_CHARS" -gt 0 ]] || die "--payload-chars must be > 0"
[[ "$PROGRESS_EVERY" =~ ^[0-9]+$ && "$PROGRESS_EVERY" -gt 0 ]] || die "--progress-every must be > 0"
[[ "$MAX_BATCHES" =~ ^[0-9]+$ && "$MAX_BATCHES" -gt 0 ]] || die "--max-batches must be > 0"

need docker
need base64
need fold
need head
need wc
need awk

TARGET_BYTES=$((TARGET_GIB * 1024 * 1024 * 1024))
HOST_BATCH_FILE="$(mktemp /tmp/mariadb-random-batch.XXXXXX.tsv)"
CONTAINER_BATCH_FILE="/tmp/$(basename "$HOST_BATCH_FILE")"

cleanup() {
  rm -f "$HOST_BATCH_FILE" >/dev/null 2>&1 || true
  sudo -n docker exec "$CONTAINER" sh -lc "rm -f '$CONTAINER_BATCH_FILE'" >/dev/null 2>&1 || true
}
trap cleanup EXIT

maria_exec() {
  sudo -n docker exec "$CONTAINER" mariadb \
    --ssl=0 \
    --local-infile=1 \
    --protocol=socket \
    --socket="$SOCKET_PATH" \
    -u"$ROOT_USER" \
    -p"$ROOT_PASSWORD" \
    "$@"
}

maria_exec_stdin() {
  sudo -n docker exec -i "$CONTAINER" mariadb \
    --ssl=0 \
    --local-infile=1 \
    --protocol=socket \
    --socket="$SOCKET_PATH" \
    -u"$ROOT_USER" \
    -p"$ROOT_PASSWORD" \
    "$@"
}

sql_scalar_retry() {
  local sql="$1"
  local attempt out
  for attempt in 1 2 3 4 5; do
    out=$(maria_exec -N -e "$sql" | tr -d '[:space:]' || true)
    if [[ -n "$out" ]]; then
      printf '%s\n' "$out"
      return 0
    fi
    sleep 1
  done
  return 1
}

sql_retry() {
  local sql="$1"
  local attempt
  for attempt in 1 2 3 4 5; do
    if maria_exec -e "$sql" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

generate_batch() {
  local raw_total=$((ROWS_PER_BATCH * RAW_BYTES_PER_ROW))

  head -c "$raw_total" /dev/urandom | base64 -w0 | fold -w "$PAYLOAD_CHARS" > "$HOST_BATCH_FILE"
  # Ensure trailing newline so wc -l reflects full row count.
  printf '\n' >> "$HOST_BATCH_FILE"

  local line_count
  line_count=$(wc -l < "$HOST_BATCH_FILE" | tr -d '[:space:]')
  [[ "$line_count" == "$ROWS_PER_BATCH" ]] || die "batch line count mismatch: expected $ROWS_PER_BATCH got $line_count"
}

log "waiting for MariaDB readiness in container=$CONTAINER"
ready=0
for i in $(seq 1 240); do
  if maria_exec -e 'SELECT 1' >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
[[ "$ready" == "1" ]] || die "MariaDB not ready in container: $CONTAINER"

SEED_SQL_FILE="$(mktemp /tmp/mariadb-random-seed.XXXXXX.sql)"
cat > "$SEED_SQL_FILE" <<SQL
CREATE DATABASE IF NOT EXISTS \`$DB_NAME\`;
USE \`$DB_NAME\`;
CREATE TABLE IF NOT EXISTS \`$TABLE_NAME\` (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  payload LONGTEXT NOT NULL
) ENGINE=InnoDB;
SQL
if [[ "$TRUNCATE_TABLE" == "1" ]]; then
  cat >> "$SEED_SQL_FILE" <<SQL
TRUNCATE TABLE \`$TABLE_NAME\`;
SQL
fi
maria_exec_stdin < "$SEED_SQL_FILE" >/dev/null
rm -f "$SEED_SQL_FILE"

log "seed ready db=$DB_NAME table=$TABLE_NAME truncate=$TRUNCATE_TABLE"
log "target_bytes=$TARGET_BYTES (~${TARGET_GIB}GiB) rows_per_batch=$ROWS_PER_BATCH payload_chars=$PAYLOAD_CHARS"

batch=0
while :; do
  batch=$((batch + 1))

  generate_batch
  sudo -n docker cp "$HOST_BATCH_FILE" "$CONTAINER:$CONTAINER_BATCH_FILE"

  if ! sql_retry "USE \`$DB_NAME\`; LOAD DATA LOCAL INFILE '$CONTAINER_BATCH_FILE' INTO TABLE \`$TABLE_NAME\` FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' (payload);"; then
    die "failed to load data batch=$batch"
  fi

  if (( batch % PROGRESS_EVERY == 0 )); then
    bytes=$(sql_scalar_retry "SELECT COALESCE(SUM(data_length + index_length),0) FROM information_schema.tables WHERE table_schema='$DB_NAME' AND table_name='$TABLE_NAME';" || echo 0)
    rows=$(sql_scalar_retry "SELECT COUNT(*) FROM \`$DB_NAME\`.\`$TABLE_NAME\`;" || echo 0)
    log "progress batch=$batch bytes=$bytes rows=$rows"

    if [[ "$bytes" =~ ^[0-9]+$ ]] && (( bytes >= TARGET_BYTES )); then
      break
    fi
  fi

  if (( batch >= MAX_BATCHES )); then
    die "reached max batches without hitting target bytes: max_batches=$MAX_BATCHES"
  fi
done

final_bytes=$(sql_scalar_retry "SELECT COALESCE(SUM(data_length + index_length),0) FROM information_schema.tables WHERE table_schema='$DB_NAME' AND table_name='$TABLE_NAME';" || echo 0)
final_rows=$(sql_scalar_retry "SELECT COUNT(*) FROM \`$DB_NAME\`.\`$TABLE_NAME\`;" || echo 0)

log "complete container=$CONTAINER db=$DB_NAME table=$TABLE_NAME bytes=$final_bytes rows=$final_rows"
printf 'container=%s\ndb=%s\ntable=%s\ntarget_bytes=%s\nfinal_bytes=%s\nfinal_rows=%s\n' \
  "$CONTAINER" "$DB_NAME" "$TABLE_NAME" "$TARGET_BYTES" "$final_bytes" "$final_rows"
