#!/usr/bin/env bash
# scripts/lib/defaults.sh — opinionated server defaults for vykar scripts
#
# All values can be overridden via the corresponding env var.
# These defaults target the standard vykar benchmark/test server.

[[ -n "${_VYKAR_DEFAULTS_LOADED:-}" ]] && return 0
_VYKAR_DEFAULTS_LOADED=1

# Paths
REPO_ROOT="${REPO_ROOT:-/mnt/repos}"
CORPUS_LOCAL="${CORPUS_LOCAL:-$HOME/corpus-local}"
CORPUS_REMOTE="${CORPUS_REMOTE:-$HOME/corpus-remote}"
RUNTIME_ROOT="${RUNTIME_ROOT:-$HOME/runtime}"
PASSPHRASE="${PASSPHRASE:-123}"

# REST backend
REST_URL="${REST_URL:-http://127.0.0.1:8585}"
REST_TOKEN="${REST_TOKEN:-${VYKAR_REST_TOKEN:-${VYKAR_TOKEN:-${VGER_TOKEN:-vger-e2e-local-token}}}}"
REST_DATA_DIR="${REST_DATA_DIR:-/mnt/repos/bench-vykar/vykar-server-data}"

# S3 / MinIO
S3_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
S3_ACCESS_KEY="${AWS_ACCESS_KEY_ID:-minioadmin}"
S3_SECRET_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
MINIO_SERVICE="${MINIO_SERVICE:-minio.service}"
MINIO_DATA_DIR="${MINIO_DATA_DIR:-/mnt/repos/bench-vykar/minio-data}"
MINIO_HEALTH_URL="${MINIO_HEALTH_URL:-http://127.0.0.1:9000/minio/health/live}"

# SFTP
SFTP_HOST="${SFTP_HOST:-127.0.0.1}"
SFTP_PORT="${SFTP_PORT:-22}"
SFTP_USER="${SFTP_USER:-$USER}"
SFTP_BASE_DIR="${SFTP_BASE_DIR:-/mnt/repos}"
SFTP_KEY="${SFTP_KEY:-$HOME/.ssh/id_ed25519}"
SFTP_KNOWN_HOSTS="${SFTP_KNOWN_HOSTS:-}"
SFTP_MAX_CONNECTIONS="${SFTP_MAX_CONNECTIONS:-}"
