# Multi-stage build for CyberDeltaEngine
FROM python:3.13-slim-bookworm AS builder

# Set working directory
WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    build-essential \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Bun
# RUN curl -fsSL https://bun.sh/install | bash

# Copy project files for building
COPY pyproject.toml setup.py README.md ./
COPY cyberdelta ./cyberdelta

# Build the wheel
RUN pip install --no-cache-dir build && \
    python -m build --wheel

# Runtime stage
FROM python:3.13-slim-bookworm

# Set working directory
WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    build-essential \
    libssl-dev \
    ca-certificates \
    nodejs \
    npm \
    zsh \
    git \
    fonts-powerline \
    fonts-dejavu-core \
    fonts-liberation \
    fonts-noto \
    fonts-noto-color-emoji \
    curl \
    nano \
    neovim \
    procps \
    wget \
    sudo \
    && fc-cache -f -v && rm -rf /var/lib/apt/lists/*

# <<< ADDED: Configure locale to support UTF-8 characters for themes
RUN apt-get update && apt-get install -y locales && \
    sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# Copy and install the built wheel
COPY --from=builder /build/dist/*.whl ./
RUN pip install --no-cache-dir *.whl && rm *.whl

# Install additional runtime dependencies not in wheel
RUN pip install --no-cache-dir uvloop

RUN curl -fsSL https://bodo.run/yek.sh | bash

# The --unattended flag prevents it from trying to chsh or start a zsh session
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended && \
    sed -i 's/ZSH_THEME="robbyrussell"/ZSH_THEME="agnoster"/' ~/.zshrc

RUN npm install -g @anthropic-ai/claude-code
RUN npm install -g @google/gemini-cli
RUN npm install -g @upstash/context7-mcp

# Create necessary directories
RUN mkdir -p /app/data/state_backups /app/logs /app/config

# Copy application files
COPY main.py ./
COPY cyberdelta ./cyberdelta

# Create non-root user with /workspaces permissions and sudo access
RUN useradd -m -u 1000 -s /bin/zsh trader && \
    mkdir -p /workspaces && \
    chown -R trader:trader /app /workspaces && \
    echo "trader ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# Copy the .zshrc and .oh-my-zsh config from root to the new user's home directory
RUN cp /root/.zshrc /home/trader/.zshrc && \
    cp -r /root/.oh-my-zsh /home/trader/.oh-my-zsh && \
    chown -R trader:trader /home/trader/.zshrc /home/trader/.oh-my-zsh

# Switch to non-root user
USER trader

# Create a custom aliases file that will be sourced by .zshrc
RUN mkdir -p /home/trader/.config && \
    echo '# Claude shortcuts' > /home/trader/.config/claude_aliases.zsh && \
    echo 'alias yolo="claude --dangerously-skip-permissions"' >> /home/trader/.config/claude_aliases.zsh && \
    echo 'alias claude-yolo="claude --dangerously-skip-permissions"' >> /home/trader/.config/claude_aliases.zsh && \
    echo '' >> /home/trader/.zshrc && \
    echo '# Source custom aliases' >> /home/trader/.zshrc && \
    echo 'source ~/.config/claude_aliases.zsh' >> /home/trader/.zshrc

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8050', timeout=5)" || exit 1

# Expose dashboard port
EXPOSE 8050

# Default command
ENTRYPOINT ["python", "-u", "main.py"]
