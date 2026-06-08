#!/usr/bin/env bash
set -e

echo "Updating package index..."
apt-get update -y

echo "Installing required system packages..."
apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    build-essential \
    curl \
    ca-certificates \
    tar

echo "Upgrading pip..."
pip install --upgrade pip

echo "Installing kafka-python..."
pip install kafka-python

echo "Environment preparation complete!"
