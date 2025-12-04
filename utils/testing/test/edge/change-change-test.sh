#!/bin/bash

# Тест вызывает пинг-понг, при котором два кластера начинают обмениваться изменениями, пока одно из них не победит.
# Корректное поведение: ping-pong не произошел, записалось последнее записанное значение(в идеальных условиях без отсутствия каких-либо сетевых лагов это будет new)
echo "
create /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test
" | ./zkCli.sh -server 2a00:b4c0:1c1::11be:0:2181

sleep 0.1

echo "
create /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test
" | ./zkCli.sh -server 10.205.111.24:2181

echo "
set /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test old
" | ./zkCli.sh -server 2a00:b4c0:1c1::11be:0:2181 & echo "
set /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test new
" | ./zkCli.sh -server 10.205.111.24:2181

echo "
get /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test
delete /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test
" | ./zkCli.sh -server 10.205.111.24:2181
