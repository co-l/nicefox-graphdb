#!/bin/bash
set -e

# NiceFox GraphDB - First-time Setup Script
# Called by nicefox-deploy during HOOK_SETUP
# Must be run with sudo (root access)
# Idempotent - safe to run multiple times

APP_USER="nicefox-graphdb"
APP_DIR="/opt/apps/nicefox-graphdb"
DATA_DIR="/var/data/nicefox-graphdb"
BACKUP_DIR="/var/backups/nicefox-graphdb"
LOG_DIR="/var/log/nicefox-graphdb"

echo "Setting up NiceFox GraphDB..."

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
cat > /etc/cron.d/nicefox-graphdb-backup << 'EOF'
# NiceFox GraphDB Backup - every 6 hours
0 */6 * * * nicefox-graphdb /opt/apps/nicefox-graphdb/deploy/backup.sh
EOF
chmod 644 /etc/cron.d/nicefox-graphdb-backup

# Setup logrotate
echo "Setting up logrotate..."
cat > /etc/logrotate.d/nicefox-graphdb << 'EOF'
/var/log/nicefox-graphdb/*.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    create 0640 nicefox-graphdb nicefox-graphdb
}
EOF
chmod 644 /etc/logrotate.d/nicefox-graphdb

# Ensure backup script is executable
chmod +x "$APP_DIR/deploy/backup.sh"

echo "Setup complete"
