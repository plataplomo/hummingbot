# Docker Setup for CyberDeltaEngine

This guide explains how to run CyberDeltaEngine in Docker containers for better isolation and deployment.

## Prerequisites

- Docker Engine installed
- Docker Compose installed
- Your exchange API credentials
- Configuration files set up

## Quick Start

1. **Clone and prepare environment**
   ```bash
   cd /path/to/CyberDeltaEngine
   cp .env.example .env
   # Edit .env with your credentials
   ```

2. **Build and run**
   ```bash
   docker-compose up --build
   ```

3. **Access dashboard**
   - Open http://localhost:8050 in your browser

## Configuration

### Environment Variables

Create a `.env` file from `.env.example`:
```bash
# Required for Hyperliquid
HL_WALLET_ADDRESS=0x...
HL_PRIVATE_KEY=0x...

# Required for Backpack
BP_API_KEY=your-api-key
BP_API_SECRET=your-api-secret
```

### Secrets Management

Two options for managing secrets:

1. **Using existing secrets.yaml** (Recommended)
   - Keep your `~/.cyberdelta/secrets.yaml` file
   - Docker will mount it read-only

2. **Using environment variables**
   - Set credentials in `.env` file
   - Less secure but simpler for testing

### Volume Mounts

The Docker setup uses these volumes:
- `./config:/app/config:ro` - Configuration files (read-only)
- `~/.cyberdelta:/home/trader/.cyberdelta:ro` - Secrets (read-only)
- `cyberdelta-data:/app/data` - Persistent state data
- `./logs:/app/logs` - Log files

## Common Commands

### Development

```bash
# Build and run with live logs
docker-compose up --build

# Run in background
docker-compose up -d

# View logs
docker-compose logs -f

# Stop containers
docker-compose down

# Stop and remove volumes (WARNING: deletes data)
docker-compose down -v
```

### Production

```bash
# Run with production settings
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# Update to latest version
docker-compose pull
docker-compose up -d

# Backup data volume
docker run --rm -v cyberdelta-data:/data -v $(pwd):/backup alpine tar czf /backup/data-backup.tar.gz -C /data .
```

## Monitoring

### Dashboard
- Accessible at http://localhost:8050
- Shows real-time trading metrics
- Performance visualization

### Logs
```bash
# View all logs
docker-compose logs

# Follow specific service
docker-compose logs -f cyberdelta

# Export logs
docker-compose logs > trading-logs.txt
```

### Health Checks
```bash
# Check container health
docker-compose ps

# Inspect health status
docker inspect cyberdelta-engine | jq '.[0].State.Health'
```

## Troubleshooting

### Container won't start
1. Check logs: `docker-compose logs`
2. Verify config files exist
3. Ensure secrets are accessible
4. Check port 8050 isn't in use

### Permission errors
- Ensure files are readable by UID 1000 (trader user)
- Check volume mount permissions

### Connection issues
- Verify API credentials in `.env` or `secrets.yaml`
- Check network connectivity from container
- Ensure exchanges aren't blocking Docker IPs

## Security Notes

1. **Never commit `.env` files** - Added to `.gitignore`
2. **Use read-only mounts** for configs and secrets
3. **Run as non-root user** (trader, UID 1000)
4. **Resource limits** prevent runaway containers
5. **Network isolation** via Docker networks

## Advanced Usage

### Custom Dockerfile
Modify the multi-stage build for your needs:
- Add additional Python packages
- Include custom scripts
- Adjust base image

### Scaling
For multiple strategies:
```yaml
services:
  strategy1:
    extends: cyberdelta
    environment:
      - STRATEGY_NAME=funding_arbitrage

  strategy2:
    extends: cyberdelta
    environment:
      - STRATEGY_NAME=another_strategy
```

### Kubernetes
See `k8s/` directory for Kubernetes deployment examples (if available).
