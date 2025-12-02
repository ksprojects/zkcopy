java -jar zkcopy.jar \
  --source 2a00:b4c0:1c1::1241:0:2181,2a00:b4c0:8c1::1d6e:0:2181,2a00:b4c0:3c1::2552:0:2181/ichwill-zen/production \
  --target fd00:b4c4:c111:101:1:0:425d:0:2181,fd00:b4c4:c110:101:1:0:1859:0:2181,fd00:b4c4:c106:102:2:0:1cf:0:2181/ichwill-zen/production \
  -w 40 \
  --syncMode \
  --withMetrics \
  --metricsUrl http://vmagent.mon.one-infra.ru/api/v1/import/prometheus \
  --cloud HC \
  --ctype prod \
    >> zksync.log &