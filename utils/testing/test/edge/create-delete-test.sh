#!/bin/bash

# Тест вызывает сильнейший пинг-понг из которого победителем выходит удаление.
# Корректное поведение: ping-pong не произошел, удаление выполнено последним
echo "
create /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test
delete /ichwill-zen/prestable/dynproperties-ok-friends-gateway/test
" | ./zkCli.sh -server 2a00:b4c0:1c1::11be:0:2181
