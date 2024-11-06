# Metalmind Deployment Guide

## System Architecture
Metalmind is deployed as a Python web application running under systemd, with nginx as a reverse proxy. The application uses gunicorn as the WSGI server.

## Directory Structure
```
/home/ubuntu/metalmind/          # Main application directory
├── static/                      # Static files served by nginx
├── secrets/                     # API keys and credentials
├── scripts/                     # The application
└── data/                        # Cached application data for rebuilding
```

## Configuration Files
- Systemd service: `/etc/systemd/system/metalmind.service`
- Nginx config: `/etc/nginx/sites-enabled/metalmind`

## Starting and Stopping the Service
```bash
sudo systemctl start|stop|restart metalmind

# Reload systemd after service file changes
sudo systemctl daemon-reload
```

## Nginx Management
```bash
# Reload nginx after config changes
sudo systemctl reload nginx

# Restart nginx completely
sudo systemctl restart nginx
```

## Logs and Troubleshooting

### View Application Logs
```bash
# View recent logs
journalctl -u metalmind [-f for tail -f behavior] [-b for since last boot]
```

### Nginx Logs
```bash
# Access logs
tail -f /var/log/nginx/access.log

# Error logs
tail -f /var/log/nginx/error.log
```

## Deployment Steps for Updates

1. Pull the latest code:
```bash
cd /home/ubuntu/metalmind
git pull
```

2. Update dependencies if requirements.txt changed:
```bash
pip install -r requirements.txt
```

3. Restart the service:
```bash
sudo systemctl restart metalmind
```
