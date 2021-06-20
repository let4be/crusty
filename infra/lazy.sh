#!/bin/bash

set -e

if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root"
   exit 1
fi

echo "About to install must have dependencies..."

apt-get update
apt-get -y install bmon net-tools git curl

echo "Installing docker..."
curl -fsSL https://get.docker.com | bash -s

echo "Installing docker-compose..."
curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

docker-compose build
