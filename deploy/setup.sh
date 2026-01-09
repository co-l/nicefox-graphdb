#!/bin/bash
set -e

# LeanGraph - First-time Setup Script
# Called by deploy script during HOOK_SETUP
# Must be run with sudo (root access)
# Idempotent - safe to run multiple times

APP_USER="leangraph"
APP_DIR="/opt/apps/leangraph"
DATA_DIR="/var/data/leangraph"
BACKUP_DIR="/var/backups/leangraph"
LOG_DIR="/var/log/leangraph"

echo "Setting up LeanGraph..."

# Create data directories
echo "Creating data directories..."
mkdir -p "$DATA_DIR"/{production,test}
chown -R "$APP_USER:$APP_USER" "$DATA_DIR"
chmod 750 "$DATA_DIR"

# Create backup directory
echo "Creating backup directory..."
mkdir -p "$BACKUP_DIR"
chown -R "$APP_USER:$APP_USER" "$BACKUP_DIR"
chmod 750 "$BACKUP_DIR"

# Create log directory
echo "Creating log directory..."
mkdir -p "$LOG_DIR"
chown -R "$APP_USER:$APP_USER" "$LOG_DIR"
chmod 750 "$LOG_DIR"

# Setup backup cron job
echo "Setting up backup cron job..."
cat > /etc/cron.d/leangraph-backup << 'EOF'
# LeanGraph Backup - every 6 hours
0 */6 * * * leangraph /opt/apps/leangraph/deploy/backup.sh
EOF
chmod 644 /etc/cron.d/leangraph-backup

# Setup logrotate
echo "Setting up logrotate..."
cat > /etc/logrotate.d/leangraph << 'EOF'
/var/log/leangraph/*.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    create 0640 leangraph leangraph
}
EOF
chmod 644 /etc/logrotate.d/leangraph

# Ensure backup script is executable
chmod +x "$APP_DIR/deploy/backup.sh"

echo "Setup complete"
