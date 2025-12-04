#!/bin/bash

# Тест создает узел на старом зукипере и пытается получить его с нового через 1 секунду.
# Корректное поведение: с нового зукипера должно быть получено значение "123"
echo "
create /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test 123
" | ./zkCli.sh -server 2a00:b4c0:1c1::11be:0:2181

sleep 1

echo "
get /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test
" | ./zkCli.sh -server 10.205.111.24:2181
