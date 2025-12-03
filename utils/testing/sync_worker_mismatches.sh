#!/bin/bash

SOURCE="2a00:b4c0:1c1::11be:0:2181"
TARGET="fd00:b4c4:c111:101:1:0:3dd5:0:2181"
ZKCOPY_JAR="zkcopy.jar"
JOBS=30

PATHS_TO_TARGET=(
"/ichwill-zen/prestable/dynproperties-publishers/get_agency_info_from_http_client_enabled-boolean"
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
