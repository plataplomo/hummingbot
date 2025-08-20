#!/bin/bash
# Script to set up conda locally in a Hummingbot worktree
# This creates an isolated conda installation per worktree

set -e

echo "==================================="
echo "Hummingbot Worktree Conda Setup"
echo "==================================="

# Check if we're in a worktree that looks like Hummingbot
if [ ! -f "bin/hummingbot.py" ]; then
    echo "❌ Error: This doesn't look like a Hummingbot worktree."
    echo "   Please run this script from inside a Hummingbot worktree directory."
    echo "   Expected to find: bin/hummingbot.py"
    exit 1
fi

# Check if conda is already installed in this worktree
if [ -d "./.conda" ]; then
    echo "⚠️  Conda already exists in this worktree at ./.conda"
    read -p "Do you want to reinstall? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Exiting without changes."
        exit 0
    fi
    echo "Removing existing conda installation..."
    rm -rf ./.conda
fi

# Download and install Miniconda locally in this worktree
echo ""
echo "📥 Downloading Miniconda..."
curl -sSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o miniconda.sh

echo "📦 Installing Miniconda to ./.conda..."
bash miniconda.sh -b -p ./.conda
rm miniconda.sh

echo "🔧 Setting up conda..."
./.conda/bin/conda init zsh
./.conda/bin/conda config --set auto_activate_base false

echo ""
echo "🐍 Creating Hummingbot environment from environment.yml..."
# Use Hummingbot's official environment.yml for proper setup
./.conda/bin/conda env create -f setup/environment.yml

echo ""
echo "📦 Installing additional Hummingbot dependencies..."
# Activate environment and install remaining packages
source ./.conda/bin/activate hummingbot

# Add the project directory to module search paths (important for Hummingbot)
conda develop .

# Install pip packages that need to be outside the environment file
python -m pip install --no-deps -r setup/pip_packages.txt

# Install pre-commit hooks if available
if command -v pre-commit &> /dev/null; then
    pre-commit install
fi

# Compile Cython files for better performance
echo "🔧 Compiling Cython modules..."
python setup.py build_ext --inplace

# Deactivate for now
conda deactivate

echo ""
echo "✅ Setup complete!"
echo ""
echo "==================================="
echo "How to use:"
echo "==================================="
echo "1. Restart your shell or run: source ~/.zshrc"
echo "2. Navigate to this worktree"
echo "3. Activate the environment: conda activate hummingbot"
echo "4. Run Hummingbot: python bin/hummingbot.py"
echo ""
echo "This conda installation is isolated to this worktree."
echo "Location: $(pwd)/.conda"
echo "==================================="

# Add to .gitignore if not already there
if [ -f ".gitignore" ]; then
    if ! grep -q "^\.conda/$" .gitignore; then
        echo "" >> .gitignore
        echo "# Local conda installation" >> .gitignore
        echo ".conda/" >> .gitignore
        echo "✅ Added .conda/ to .gitignore"
    fi
fi
