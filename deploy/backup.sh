#!/bin/bash
set -euo pipefail

# LeanGraph Backup Script
# This script performs hot backups of all production databases
# Called by cron job installed by deploy/setup.sh

# Configuration (override via environment variables)
DATA_PATH="${DATA_PATH:-/var/data/leangraph}"
BACKUP_PATH="${BACKUP_PATH:-/var/backups/leangraph}"
KEEP_COUNT="${KEEP_COUNT:-10}"
KEEP_DAYS="${KEEP_DAYS:-30}"
LOG_FILE="${LOG_FILE:-/var/log/leangraph/backup.log}"
APP_PATH="${APP_PATH:-/opt/apps/leangraph}"

# Ensure log directory exists
mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $1" | tee -a "$LOG_FILE" >&2
}

# Check if required directories exist
if [ ! -d "$DATA_PATH" ]; then
    error "Data path does not exist: $DATA_PATH"
    exit 1
fi

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_PATH"

log "=========================================="
log "Starting LeanGraph Backup"
log "Data path: $DATA_PATH"
log "Backup path: $BACKUP_PATH"
log "Keep count: $KEEP_COUNT per project"
log "=========================================="

# Run backup using CLI
cd "$APP_PATH"

if [ -f "dist/cli.js" ]; then
    # Production: use compiled version
    CLI_CMD="node dist/cli.js"
else
    # Development: use tsx
    CLI_CMD="npx tsx src/cli.ts"
fi

$CLI_CMD backup \
    --data "$DATA_PATH" \
    --output "$BACKUP_PATH" \
    --keep "$KEEP_COUNT" \
    2>&1 | tee -a "$LOG_FILE"

BACKUP_EXIT_CODE=${PIPESTATUS[0]}

if [ $BACKUP_EXIT_CODE -ne 0 ]; then
    error "Backup command failed with exit code: $BACKUP_EXIT_CODE"
    exit $BACKUP_EXIT_CODE
fi

# Clean up old backups beyond retention period
log "Cleaning up backups older than $KEEP_DAYS days..."
DELETED_COUNT=$(find "$BACKUP_PATH" -name "*.db" -mtime +$KEEP_DAYS -delete -print 2>/dev/null | wc -l || echo "0")
log "Deleted $DELETED_COUNT old backup files"

# Show backup status
log "Current backup status:"
$CLI_CMD backup --status --output "$BACKUP_PATH" 2>&1 | tee -a "$LOG_FILE"

log "=========================================="
log "Backup completed successfully"
log "=========================================="

# Optional: Add remote sync commands here
# Example with rclone:
# if command -v rclone &> /dev/null; then
#     log "Syncing to remote storage..."
#     rclone sync "$BACKUP_PATH" remote:leangraph-backups/ --log-file="$LOG_FILE"
# fi

# Example with aws cli:
# if command -v aws &> /dev/null; then
#     log "Syncing to S3..."
#     aws s3 sync "$BACKUP_PATH" s3://your-bucket/leangraph-backups/
# fi
