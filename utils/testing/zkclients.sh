#!/bin/bash

ZK_HOST=$1
ZK_PORT=${2:-2181}

CONNECTIONS=$(echo cons | nc "$ZK_HOST" "$ZK_PORT")

IPS=$(echo "$CONNECTIONS" | sed -E 's|^ */\[?||; s|\]?:[0-9]+\[.*||')

for IP in $IPS; do
    HOSTNAME=$(nslookup "$IP" | grep name | sed -e 's/.*= //g')

    if [ -n "$HOSTNAME" ]; then
        echo "$HOSTNAME"
    else
        echo "$IP"
    fi
done
