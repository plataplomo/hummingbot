# Hummingbot Conda Setup Guide

## Overview
This guide explains how to set up isolated conda environments for Hummingbot worktrees while keeping CyberDelta using UV + Python 3.13.5.

## Architecture

```
CyberDeltaEngine/
├── cyberdelta/           # Uses UV + Python 3.13.5
├── vendor/
│   └── hummingbot/       # Submodule
└── worktrees/
    └── hummingbot-dev/   # Each worktree gets its own conda
        ├── conda/        # Local conda installation (gitignored)
        ├── hummingbot/   # Source code
        └── requirements.txt
```

## Setup Instructions

### 1. Navigate to your Hummingbot worktree
```bash
cd /workspaces/CyberDeltaEngine/worktrees/hummingbot-add/worktrees/hummingbot-dev
```

### 2. Run the setup script
```bash
../../scripts/setup_hummingbot_conda.sh
```

This script will:
- Download and install Miniconda **locally** in `./conda/`
- Create a `hummingbot` environment with Python 3.10
- Install all Hummingbot dependencies
- Add `conda/` to `.gitignore`

### 3. Activate the environment
```bash
# Restart your shell first
source ~/.zshrc

# Then activate
conda activate hummingbot
```

### 4. Verify installation
```bash
python --version  # Should show Python 3.10.x
python -c "import hummingbot; print('✅ Hummingbot ready')"
```

## Key Benefits

1. **Complete Isolation**: Each worktree has its own conda installation
2. **No Permission Issues**: Everything is in user-owned directories
3. **Clean Git**: The `conda/` folder is gitignored
4. **No Conflicts**: CyberDelta keeps using Python 3.13.5 with UV

## Daily Workflow

### Working on CyberDelta
```bash
cd /workspaces/CyberDeltaEngine
# Uses Python 3.13.5 automatically
uv sync
python main.py
```

### Working on Hummingbot
```bash
cd /workspaces/CyberDeltaEngine/worktrees/hummingbot-add/worktrees/hummingbot-dev
conda activate hummingbot  # Switch to Python 3.10
python bin/hummingbot.py
```

### Switching between them
```bash
# In Hummingbot worktree with conda activated
conda deactivate  # Back to Python 3.13.5

# Or just cd to different directory - conda env is directory-aware
```

## Multiple Worktrees

If you create another Hummingbot worktree:
```bash
cd /workspaces/CyberDeltaEngine/worktrees/hummingbot-add
git worktree add worktrees/hummingbot-experiment feature/new-feature

cd worktrees/hummingbot-experiment
../../scripts/setup_hummingbot_conda.sh  # Set up conda for this worktree too
```

Each worktree gets its own isolated conda installation.

## Cleanup

To remove conda from a worktree:
```bash
rm -rf ./conda
# Also remove conda init lines from ~/.zshrc if no longer needed
```

## Troubleshooting

### "conda: command not found"
- Make sure you're in a worktree where you ran the setup script
- Run `source ~/.zshrc` to reload shell config

### "Python 3.13 not found" after conda activate
- This is expected! Conda environment uses Python 3.10
- Run `conda deactivate` to return to system Python 3.13

### Permission denied errors
- The setup script installs everything in user-owned directories
- No sudo or special permissions needed

## Summary

- **CyberDelta**: UV + Python 3.13.5 (system-wide)
- **Hummingbot**: Conda + Python 3.10 (per-worktree)
- **No conflicts**: Completely isolated environments
- **Git-friendly**: All conda installations are gitignored
