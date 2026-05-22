#!/bin/sh
# =============================================================================
# Relier Redis, periodic snapshot backup with rotation.
#
# Runs as a sidecar container. Triggers BGSAVE against a *replica* (never the
# master, to keep the write path unloaded), archives the resulting RDB file to
# a dedicated backup volume, rotates old archives, and if BACKUP_S3_BUCKET is
# set and the `aws` CLI is present uploads a copy to S3-compatible storage.
#
# Environment:
#   BACKUP_REDIS_HOST        replica to snapshot        (default: relier-redis-replica-1)
#   BACKUP_REDIS_PORT        replica port               (default: 6379)
#   BACKUP_SRC_DIR           replica data dir (RO mount) (default: /data)
#   BACKUP_DIR               archive destination        (default: /backups)
#   BACKUP_INTERVAL_SECONDS  seconds between passes      (default: 3600)
#   BACKUP_RETENTION         archives to keep            (default: 168 = 7d hourly)
#   BACKUP_S3_BUCKET         optional S3 bucket for offsite copies
# =============================================================================
set -eu

REDIS_HOST="${BACKUP_REDIS_HOST:-relier-redis-replica-1}"
REDIS_PORT="${BACKUP_REDIS_PORT:-6379}"
SRC_DIR="${BACKUP_SRC_DIR:-/data}"
BACKUP_DIR="${BACKUP_DIR:-/backups}"
INTERVAL="${BACKUP_INTERVAL_SECONDS:-3600}"
RETENTION="${BACKUP_RETENTION:-168}"

log() { echo "[redis-backup] $(date -u +%Y-%m-%dT%H:%M:%SZ) $*"; }

run_backup() {
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  log "Triggering BGSAVE on ${REDIS_HOST}:${REDIS_PORT}"

  # LASTSAVE returns the unix time of the last successful save. We snapshot it,
  # ask for a BGSAVE, then wait for the value to advance, that is the only
  # reliable signal that the fork-and-write completed.
  before="$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" LASTSAVE)"
  redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" BGSAVE >/dev/null

  saved=""
  for _ in $(seq 1 120); do
    now="$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" LASTSAVE)"
    if [ "$now" != "$before" ]; then
      saved="yes"
      break
    fi
    sleep 1
  done
  if [ -z "$saved" ]; then
    log "ERROR: BGSAVE did not complete within 120s, skipping this pass"
    return 1
  fi

  archive="${BACKUP_DIR}/relier-redis-${ts}.rdb.gz"
  if [ ! -f "${SRC_DIR}/dump.rdb" ]; then
    log "ERROR: ${SRC_DIR}/dump.rdb not found, is the replica data volume mounted?"
    return 1
  fi
  gzip -c "${SRC_DIR}/dump.rdb" > "$archive"
  log "Wrote ${archive} ($(wc -c < "$archive") bytes)"

  if [ -n "${BACKUP_S3_BUCKET:-}" ]; then
    if command -v aws >/dev/null 2>&1; then
      aws s3 cp "$archive" "s3://${BACKUP_S3_BUCKET}/relier-redis/$(basename "$archive")"
      log "Uploaded to s3://${BACKUP_S3_BUCKET}/relier-redis/"
    else
      log "WARN: BACKUP_S3_BUCKET is set but the 'aws' CLI is missing from this image"
    fi
  fi

  # Rotation: keep only the newest $RETENTION archives.
  count="$(find "$BACKUP_DIR" -name 'relier-redis-*.rdb.gz' | wc -l)"
  if [ "$count" -gt "$RETENTION" ]; then
    ls -1t "${BACKUP_DIR}"/relier-redis-*.rdb.gz | tail -n +"$((RETENTION + 1))" | while read -r old; do
      rm -f "$old"
      log "Rotated out $(basename "$old")"
    done
  fi
}

mkdir -p "$BACKUP_DIR"
log "Backup loop started - host=${REDIS_HOST} interval=${INTERVAL}s retention=${RETENTION}"
while true; do
  run_backup || log "backup pass failed, will retry next interval"
  sleep "$INTERVAL"
done
