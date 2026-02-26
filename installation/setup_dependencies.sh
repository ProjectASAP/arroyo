#!/bin/bash

set -e

echo "Setting up Arroyo development dependencies..."

# Install system dependencies for Ubuntu
echo "Installing system packages..."
sudo apt-get update
sudo apt-get install -y pkg-config build-essential libssl-dev openssl cmake curl \
    postgresql postgresql-client protobuf-compiler git

# Start PostgreSQL service
echo "Starting PostgreSQL service..."
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Install NVM and Node.js 18
echo "Installing NVM and Node.js 18..."
if [ ! -d "$HOME/.nvm" ]; then
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash
    export NVM_DIR="$HOME/.nvm"
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
    [ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"
else
    echo "NVM already installed"
    export NVM_DIR="$HOME/.nvm"
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
fi

# Install Node.js 18
nvm install 18
nvm use 18
nvm alias default 18

# Install Rust if not already installed
echo "Installing Rust..."
if ! command -v rustc &> /dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
else
    echo "Rust already installed"
    source "$HOME/.cargo/env"
fi

# Install refinery CLI
echo "Installing refinery CLI..."
cargo install refinery_cli

# Install pnpm
echo "Installing pnpm..."
if ! command -v pnpm &> /dev/null; then
    curl -fsSL https://get.pnpm.io/install.sh | sh -
    source ~/.bashrc
    sudo cp "$HOME/.local/share/pnpm/pnpm" /usr/local/bin/pnpm
    sudo cp -r "$HOME/.local/share/pnpm/.tools" /usr/local/bin/
else
    echo "pnpm already installed"
fi

# Set up PostgreSQL database and user for Arroyo
echo "Setting up PostgreSQL database for Arroyo..."
# do this only if User arroyo does not exist
sudo -u postgres psql -c "SELECT 1 FROM pg_roles WHERE rolname='arroyo'" | grep -q 1 || \
    sudo -u postgres psql -c "CREATE ROLE arroyo WITH LOGIN PASSWORD 'arroyo';"
#sudo -u postgres psql -c "CREATE USER arroyo WITH PASSWORD 'arroyo' SUPERUSER;"
# do this only if Database arroyo does not exist
sudo -u postgres psql -c "SELECT 1 FROM pg_database WHERE datname='arroyo'" | grep -q 1 || \
    # Create the database if it does not exist
    sudo -u postgres createdb arroyo

echo "Arroyo dependencies setup completed!"

sudo apt install -y linux-tools-common linux-tools-generic "linux-tools-$(uname -r)"
cargo install flamegraph
echo "Installed flamegraph for profiling"
