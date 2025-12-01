java -jar zkcopy.jar \
  --source 2a00:b4c0:1c1::11be:0:2181,2a00:b4c0:3c1::22d7:0:2181,2a00:b4c0:8c1::1b85:0:2181/ichwill-zen/prestable \
  --target fd00:b4c4:c111:101:1:0:3dd5:0:2181,fd00:b4c4:c110:101:1:0:1877:0:2181,fd00:b4c4:c106:102:2:0:cc:0:2181/ichwill-zen/prestable \
  -w 40 \
  --syncMode \
  --withMetrics \
  --metricsUrl http://vmagent.mon.one-infra.ru/api/v1/import/prometheus \
  --cloud HC \
  --ctype test \
    > zksync.log &