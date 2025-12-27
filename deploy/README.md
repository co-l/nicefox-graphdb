# NiceFox GraphDB - Deployment Guide

Deploy NiceFox GraphDB to a Debian 13 VPS with nginx reverse proxy, systemd service, and automated backups.

## Prerequisites

- Debian 13 (Trixie) VPS
- Domain pointing to your server: `graphdb.nicefox.net`
- Root or sudo access

## 1. System Setup

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install required packages
sudo apt install -y curl git nginx certbot python3-certbot-nginx

# Install Node.js 22 LTS
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
sudo apt install -y nodejs

# Install pnpm
sudo npm install -g pnpm

# Verify installations
node --version  # Should be v22.x
pnpm --version
```

## 2. Create Application User

```bash
# Create dedicated user for the service
sudo useradd -r -m -s /bin/bash graphdb

# Create required directories
sudo mkdir -p /opt/nicefox-graphdb
sudo mkdir -p /var/data/nicefox-graphdb/{production,test}
sudo mkdir -p /var/backups/nicefox-graphdb
sudo mkdir -p /var/log/nicefox-graphdb

# Set ownership
sudo chown -R graphdb:graphdb /opt/nicefox-graphdb
sudo chown -R graphdb:graphdb /var/data/nicefox-graphdb
sudo chown -R graphdb:graphdb /var/backups/nicefox-graphdb
sudo chown -R graphdb:graphdb /var/log/nicefox-graphdb
```

## 3. Deploy Application

```bash
# Switch to graphdb user
sudo -u graphdb -i

# Clone repository (or copy files)
cd /opt/nicefox-graphdb
git clone https://github.com/co-l/nicefox-graphdb.git .

# Install dependencies
pnpm install

# Build packages
pnpm build

# Exit back to your user
exit
```

## 4. Configuration

Create the environment file:

```bash
sudo nano /opt/nicefox-graphdb/.env
```

Add:

```env
# Server Configuration
PORT=3000
HOST=127.0.0.1
DATA_PATH=/var/data/nicefox-graphdb
BACKUP_PATH=/var/backups/nicefox-graphdb

# API Keys (generate with: openssl rand -hex 16)
# Format: KEY=project:env or KEY=admin for admin access
API_KEY_ADMIN=your-admin-key-here
API_KEY_PROJECT1=your-project1-key-here
```

Set permissions:

```bash
sudo chown graphdb:graphdb /opt/nicefox-graphdb/.env
sudo chmod 600 /opt/nicefox-graphdb/.env
```

## 5. Systemd Service

Create the service file:

```bash
sudo nano /etc/systemd/system/nicefox-graphdb.service
```

Add:

```ini
[Unit]
Description=NiceFox GraphDB Server
After=network.target

[Service]
Type=simple
User=graphdb
Group=graphdb
WorkingDirectory=/opt/nicefox-graphdb
# API keys are automatically loaded from /var/data/nicefox-graphdb/api-keys.json
ExecStart=/usr/bin/node /opt/nicefox-graphdb/packages/cli/dist/index.js serve --port 3000 --host 127.0.0.1 --data /var/data/nicefox-graphdb
Restart=always
RestartSec=10
StandardOutput=append:/var/log/nicefox-graphdb/stdout.log
StandardError=append:/var/log/nicefox-graphdb/stderr.log

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/data/nicefox-graphdb /var/log/nicefox-graphdb
PrivateTmp=true

# Environment
Environment=NODE_ENV=production

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable nicefox-graphdb
sudo systemctl start nicefox-graphdb

# Check status
sudo systemctl status nicefox-graphdb

# View logs
sudo journalctl -u nicefox-graphdb -f
```

## 6. Nginx Configuration

Create nginx config:

```bash
sudo nano /etc/nginx/sites-available/graphdb.nicefox.net
```

Add:

```nginx
server {
    listen 80;
    server_name graphdb.nicefox.net;

    # Redirect HTTP to HTTPS
    location / {
        return 301 https://$server_name$request_uri;
    }
}

server {
    listen 443 ssl;
    http2 on;
    server_name graphdb.nicefox.net;

    # SSL will be configured by certbot
    # ssl_certificate /etc/letsencrypt/live/graphdb.nicefox.net/fullchain.pem;
    # ssl_certificate_key /etc/letsencrypt/live/graphdb.nicefox.net/privkey.pem;

    # Security headers
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;
    add_header Referrer-Policy "strict-origin-when-cross-origin" always;

    # Logging
    access_log /var/log/nginx/graphdb.nicefox.net.access.log;
    error_log /var/log/nginx/graphdb.nicefox.net.error.log;

    # Proxy settings
    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;
        proxy_read_timeout 300s;
        proxy_connect_timeout 75s;

        # Request size limit (adjust as needed)
        client_max_body_size 10M;
    }

    # Health check endpoint (no auth required)
    location /health {
        proxy_pass http://127.0.0.1:3000/health;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
    }
}
```

Enable site and get SSL certificate:

```bash
# Enable site
sudo ln -s /etc/nginx/sites-available/graphdb.nicefox.net /etc/nginx/sites-enabled/

# Test nginx config
sudo nginx -t

# Reload nginx
sudo systemctl reload nginx

# Get SSL certificate
sudo certbot --nginx -d graphdb.nicefox.net

# Verify auto-renewal
sudo certbot renew --dry-run
```

## 7. Backup Cron Setup

Create backup script:

```bash
sudo vi /opt/nicefox-graphdb/scripts/backup.sh
```

Add:

```bash
#!/bin/bash
set -euo pipefail

# Configuration
DATA_PATH="/var/data/nicefox-graphdb"
BACKUP_PATH="/var/backups/nicefox-graphdb"
KEEP_DAYS=30
LOG_FILE="/var/log/nicefox-graphdb/backup.log"

# Timestamp
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "Starting backup..."

# Run backup using CLI
cd /opt/nicefox-graphdb
/usr/bin/node packages/cli/dist/index.js backup \
    --data "$DATA_PATH" \
    --output "$BACKUP_PATH" \
    --keep 10 \
    2>&1 | tee -a "$LOG_FILE"

# Clean up old backup logs (keep 30 days)
find "$BACKUP_PATH" -name "*.db" -mtime +$KEEP_DAYS -delete 2>/dev/null || true

log "Backup completed successfully"

# Optional: Sync to remote storage
# aws s3 sync "$BACKUP_PATH" s3://your-bucket/nicefox-graphdb-backups/
# or
# rclone sync "$BACKUP_PATH" remote:nicefox-graphdb-backups/
```

Set permissions:

```bash
sudo chmod +x /opt/nicefox-graphdb/scripts/backup.sh
sudo chown graphdb:graphdb /opt/nicefox-graphdb/scripts/backup.sh
```

Create cron job:

```bash
sudo vi /etc/cron.d/nicefox-graphdb-backup
```

Add:

```cron
# NiceFox GraphDB Backup
# Run every 6 hours
0 */6 * * * graphdb /opt/nicefox-graphdb/scripts/backup.sh

# Alternative: Run daily at 3 AM
# 0 3 * * * graphdb /opt/nicefox-graphdb/scripts/backup.sh
```

Set permissions:

```bash
sudo chmod 644 /etc/cron.d/nicefox-graphdb-backup
```

## 8. Log Rotation

Create logrotate config:

```bash
sudo vi /etc/logrotate.d/nicefox-graphdb
```

Add:

```
/var/log/nicefox-graphdb/*.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    create 0640 graphdb graphdb
    sharedscripts
    postrotate
        systemctl reload nicefox-graphdb > /dev/null 2>&1 || true
    endscript
}
```

## 9. Firewall Setup

```bash
# Install ufw if not present
sudo apt install -y ufw

# Allow SSH, HTTP, HTTPS
sudo ufw allow ssh
sudo ufw allow 'Nginx Full'

# Enable firewall
sudo ufw enable

# Verify
sudo ufw status
```

## 10. Verify Deployment

```bash
# Check service status
sudo systemctl status nicefox-graphdb

# Test health endpoint
curl https://graphdb.nicefox.net/health

# Test with API key (replace with your key)
curl -X POST https://graphdb.nicefox.net/query/production/myproject \
  -H "Authorization: Bearer your-api-key" \
  -H "Content-Type: application/json" \
  -d '{"cypher": "MATCH (n) RETURN COUNT(n) as count"}'
```

## Maintenance Commands

```bash
# View service logs
sudo journalctl -u nicefox-graphdb -f

# Restart service
sudo systemctl restart nicefox-graphdb

# Manual backup
sudo -u graphdb /opt/nicefox-graphdb/scripts/backup.sh

# Check backup status
sudo -u graphdb node /opt/nicefox-graphdb/packages/cli/dist/index.js backup --status \
  --output /var/backups/nicefox-graphdb

# List projects
sudo -u graphdb node /opt/nicefox-graphdb/packages/cli/dist/index.js list \
  --data /var/data/nicefox-graphdb

# Create new project (auto-generates API key)
sudo -u graphdb node /opt/nicefox-graphdb/packages/cli/dist/index.js create compta \
  --data /var/data/nicefox-graphdb

# Add additional API key for a project
sudo -u graphdb node /opt/nicefox-graphdb/packages/cli/dist/index.js apikey add compta \
  --data /var/data/nicefox-graphdb

# Add production-only API key
sudo -u graphdb node /opt/nicefox-graphdb/packages/cli/dist/index.js apikey add compta \
  --env production --data /var/data/nicefox-graphdb

# Add admin API key
sudo -u graphdb node /opt/nicefox-graphdb/packages/cli/dist/index.js apikey add admin \
  --admin --data /var/data/nicefox-graphdb

# List API keys
sudo -u graphdb node /opt/nicefox-graphdb/packages/cli/dist/index.js apikey list \
  --data /var/data/nicefox-graphdb

# Remove API key by prefix
sudo -u graphdb node /opt/nicefox-graphdb/packages/cli/dist/index.js apikey remove <prefix> \
  --data /var/data/nicefox-graphdb
```

## Updating the Application

```bash
# Stop service
sudo systemctl stop nicefox-graphdb

# Update code
sudo -u graphdb -i
cd /opt/nicefox-graphdb
git pull
pnpm install
pnpm build
exit

# Start service
sudo systemctl start nicefox-graphdb
```

## Troubleshooting

### Service won't start
```bash
# Check logs
sudo journalctl -u nicefox-graphdb -n 50 --no-pager

# Check file permissions
ls -la /var/data/nicefox-graphdb/
ls -la /opt/nicefox-graphdb/
```

### 502 Bad Gateway
```bash
# Check if service is running
sudo systemctl status nicefox-graphdb

# Check if port is listening
ss -tlnp | grep 3000
```

### SSL Certificate issues
```bash
# Renew certificate manually
sudo certbot renew

# Check certificate status
sudo certbot certificates
```
