#!/bin/bash

# Тест производит цепочку изменений как на старом зукипере, так и на новом, имитируя нормальное использование
# Корректное поведение: в конечном итоге должно быть получено значение 'Hello!' и удалено
echo "
create /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test 123
" | ./zkCli.sh -server 2a00:b4c0:1c1::11be:0:2181

sleep 0.5

echo "
delete /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test
" | ./zkCli.sh -server 10.205.111.24:2181

sleep 0.5

echo "
create /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test 321
" | ./zkCli.sh -server 10.205.111.24:2181

sleep 0.5

echo "
set /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test Hello!
" | ./zkCli.sh -server 2a00:b4c0:1c1::11be:0:2181

sleep 0.5

echo "
get /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test
delete /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test
" | ./zkCli.sh -server 10.205.111.24:2181
