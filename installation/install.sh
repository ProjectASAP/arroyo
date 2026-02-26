#!/bin/bash

set -e

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
PARENT_DIR=$(dirname "$THIS_DIR")

# Source environment variables
source ~/.bashrc
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
source "$HOME/.cargo/env"

# Create refinery configuration file
echo "Creating refinery configuration..."
cat > ~/refinery.toml << EOF
[main]
db_type = "Postgres"
db_host = "localhost"
db_port = "5432"
db_user = "arroyo"
db_pass = "arroyo"
db_name = "arroyo"
EOF

# Run database migrations
echo "Running database migrations..."
cd "$PARENT_DIR"
refinery migrate -c ~/refinery.toml -p crates/arroyo-api/migrations

# Build the frontend
echo "Building frontend..."
cd "$PARENT_DIR/webui"
# pnpm might give an interactive prompt about the modules directory being reinstalled
yes | pnpm install
pnpm build

# Build Arroyo binary
echo "Building Arroyo binary..."
cd "$PARENT_DIR"
cargo build --package arroyo --release

# build docker container
docker build . -f docker/Dockerfile -t arroyo-full --target arroyo-full
