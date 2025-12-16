echo "
create -e /ichwill-zen/prestable/test-service/n-0
create -e /ichwill-zen/prestable/test-service/n-1
create -e /ichwill-zen/prestable/test-service/n-2
create -e /ichwill-zen/prestable/test-service/n-3
create -e /ichwill-zen/prestable/test-service/n-4
create -e /ichwill-zen/prestable/test-service/n-5
create -e /ichwill-zen/prestable/test-service/n-6
create -e /ichwill-zen/prestable/test-service/n-7
create -e /ichwill-zen/prestable/test-service/n-8
create -e /ichwill-zen/prestable/test-service/n-9
create -e /ichwill-zen/prestable/test-service/n-10
create -e /ichwill-zen/prestable/test-service/n-11
create -e /ichwill-zen/prestable/test-service/n-12
create -e /ichwill-zen/prestable/test-service/n-13
create -e /ichwill-zen/prestable/test-service/n-14
create -e /ichwill-zen/prestable/test-service/n-15
" | ./zkCli.sh -server 2a00:b4c0:1c1::11be:0:2181

echo "
delete /ichwill-zen/prestable/test-service/n-0
delete /ichwill-zen/prestable/test-service/n-1
delete /ichwill-zen/prestable/test-service/n-2
delete /ichwill-zen/prestable/test-service/n-3
delete /ichwill-zen/prestable/test-service/n-4
delete /ichwill-zen/prestable/test-service/n-5
delete /ichwill-zen/prestable/test-service/n-6
delete /ichwill-zen/prestable/test-service/n-7
delete /ichwill-zen/prestable/test-service/n-8
delete /ichwill-zen/prestable/test-service/n-9
delete /ichwill-zen/prestable/test-service/n-10
delete /ichwill-zen/prestable/test-service/n-11
delete /ichwill-zen/prestable/test-service/n-12
delete /ichwill-zen/prestable/test-service/n-13
delete /ichwill-zen/prestable/test-service/n-14
delete /ichwill-zen/prestable/test-service/n-15
" | ./zkCli.sh -server 2a00:b4c0:1c1::11be:0:2181