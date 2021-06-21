#!/bin/bash

set -e

if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root"
   exit 1
fi

echo "About to install must have dependencies..."

apt-get update
apt-get -y install git net-tools dnsutils bmon htop nano

echo "Installing docker..."
curl -fsSL https://get.docker.com | bash -s

echo "Installing docker-compose..."
curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

groupadd docker || true
usermod -aG docker "$SUDO_USER" || true

echo "Your current network interfaces seem to be:"
ifconfig

conf_eth () {
	INDEX=$1
	IP=$2
	echo "Configuring $DEV:$INDEX with $IP"
	ifconfig $DEV:$INDEX $IP up || true
}

echo "what is the internet's NIC name(enter to skip)?"
read -r DEV </dev/tty
echo "about to configure $DEV"

echo "what are available NIC IPs(enter to skip)?"
i=0
while IFS='$\n' read -r IP </dev/tty; do
    if [ "$IP" == "" ]; then
        echo "done with the network..."
        break
    fi
    echo "about to configure $DEV with $IP"

    while true; do
        read -p "Continue(y/n)?" yn </dev/tty
        case $yn in
            [Yy]* ) conf_eth $i "$IP"; break;;
            [Nn]* ) break;;
            * ) echo "Please answer y(yes) or n(no).";;
        esac
    done

    ((i=i+1))
    echo "next ip(enter to skip)?"
done

echo "Getting crusty..."
git clone https://github.com/let4be/crusty
chown -R "$SUDO_USER":"$SUDO_USER" crusty
chmod -R go-wx crusty
cd crusty

echo "Configuring sysctl..."
cp  ./infra/sysctl.conf /etc/sysctl.d/90-crusty.conf
sysctl --system

echo "Configure Crusty pls..."
nano ./main/config.yaml </dev/tty

docker build -f ./infra/crusty/Dockerfile -t crusty_crusty .

docker-compose build

echo "Everything is almost ready to go..."
echo "To make sure your user can use docker, execute sudo -s -u ${SUDO_USER}"
echo "Or just logout and log back in"
echo "To play with crusty"
echo "CRUSTY_SEEDS=https://cnn.com docker-compose up -d --build"
