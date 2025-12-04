#!/bin/bash

# Тест инициирует создание одного и того же узла с разными значениями сразу в двух кластерах зукипера.
# Корректное поведение: Синкалка применит наиболее позднее созданное значение на оба кластера
echo "
create /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test 123
" | ./zkCli.sh -server 2a00:b4c0:1c1::11be:0:2181 & echo "
create /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test 321
" | ./zkCli.sh -server 10.205.111.24:2181

sleep 0.1

echo "
get /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test
delete /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test
" | ./zkCli.sh -server 2a00:b4c0:1c1::11be:0:2181
