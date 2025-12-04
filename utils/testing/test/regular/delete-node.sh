#!/bin/bash

# Тест удаляет узел на старом зукипере и пытается получить его с нового через 1 секунду.
# Корректное поведение: на новом зукипере значение должно отсутвтвовать(Node does not exist: /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test)
echo "
delete /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test
" | ./zkCli.sh -server 2a00:b4c0:1c1::11be:0:2181

sleep 1

echo "
get /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test
" | ./zkCli.sh -server 10.205.111.24:2181
