#!/bin/bash

SOURCE="2a00:b4c0:1c1::11be:0:2181"
TARGET="fd00:b4c4:c111:101:1:0:3dd5:0:2181"
ZKCOPY_JAR="zkcopy.jar"
JOBS=30

PATHS_TO_TARGET=(
"/ichwill-zen/prestable/dynproperties-vk-clips-mediator"
"/ichwill-zen/prestable/hermes/clients/1.vm-ai.r-l.kc.idzn.ru"
"/ichwill-zen/prestable/hermes/clients/vkclips"
"/ichwill-zen/prestable/hermes/clients/1.vm-ai.m-andronov-2.kc.idzn.ru/recommender-ucp-gateway-posts"
"/ichwill-zen/prestable/dynproperties-pubhouse-reader/pubhouse-counters-listener-do-seek-to-end-on-reassign-boolean"
"/ichwill-zen/prestable/dynproperties-pubhouse-reader/community_dashboard_user_actions-pubhouse-counters-test-is-running-boolean"
"/ichwill-zen/prestable/bazinga_pubhouse_worker/overrides/pubhouseAllCountersClickhouseSolidFixing"
"/ichwill-zen/prestable/bazinga_pubhouse_worker/overrides/pubhouseAllCountersClickhouseSolidFixingScheduler"
)

PATHS_TO_SOURCE=(
)

echo "Starting parallel sync with $JOBS jobs..."

printf "%s\n" "${PATHS_TO_TARGET[@]}" | xargs -P "$JOBS" -I {} \
    java -jar "$ZKCOPY_JAR" \
    --source "$SOURCE{}" \
    --target "$TARGET{}" \
    --workers 10
    -с

printf "%s\n" "${PATHS_TO_SOURCE[@]}" | xargs -P "$JOBS" -I {} \
    java -jar "$ZKCOPY_JAR" \
    --source "$TARGET{}" \
    --target "$SOURCE{}" \
    --workers 10
