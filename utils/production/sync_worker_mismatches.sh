#!/bin/bash

SOURCE="2a00:b4c0:1c1::1241:0:2181"
TARGET="fd00:b4c4:c110:101:1:0:1859:0:2181"
ZKCOPY_JAR="zkcopy.jar"
JOBS=30

PATHS_TO_TARGET=(
"/ichwill-zen/production/bazinga_ok_money_worker/heartbeat"
"/ichwill-zen/production/bazinga_topic_channel_worker/heartbeat"
"/ichwill-zen/production/bazinga_adtech_recommender_worker/heartbeat"
"/ichwill-zen/production/bazinga_content/heartbeat"
"/ichwill-zen/production/bazinga_music_worker/heartbeat"
"/ichwill-zen/production/bazinga_vk_video_recommender_worker/heartbeat"
"/ichwill-zen/production/bazinga_pub_worker/heartbeat"
"/ichwill-zen/production/push-bazinga/heartbeat"
"/ichwill-zen/production/bazinga_vk_clips_recommender_worker/heartbeat"
"/ichwill-zen/production/bazinga_profile_stream_worker/heartbeat"
"/ichwill-zen/production/bazinga_seo/heartbeat"
"/ichwill-zen/production/bazinga_pro_worker/heartbeat"
"/ichwill-zen/production/bazinga_trend_spotter/heartbeat"
"/ichwill-zen/production/bazinga_ucp_worker/heartbeat"
"/ichwill-zen/production/bazinga-ml/heartbeat"
"/ichwill-zen/production/bazinga_ok_discovery_recommender_worker/heartbeat"
"/ichwill-zen/production/bazinga-ab/heartbeat"
"/ichwill-zen/production/bazinga_ucp_content/heartbeat"
"/ichwill-zen/production/bazinga_pubstats/heartbeat"
"/ichwill-zen/production/bazinga-common/heartbeat"
"/ichwill-zen/production/bazinga_vk_ozon_recommender_worker/heartbeat"
"/ichwill-zen/production/bazinga_vk_feed_recommender_worker/heartbeat"
"/ichwill-zen/production/bazinga_pubhouse_worker/heartbeat"
"/ichwill-zen/production/bazinga_dzen_recommender_worker/heartbeat"
"/ichwill-zen/production/bazinga_ucp_dzen_recommender_worker_recommender_worker/heartbeat"
"/ichwill-zen/production/bazinga_recommender_wizard_worker/heartbeat"
"/ichwill-zen/production/bazinga_comments_worker/heartbeat"
"/ichwill-zen/production/bazinga_isec_worker/heartbeat"
"/ichwill-zen/production/dynproperties-dzen-news-base-recommender/dragonfly-total-limit-string"
"/ichwill-zen/production/dynproperties-bifrost-item-counters-vk-video/measure-file-cache-size-boolean"
"/ichwill-zen/production/dynproperties-dzen-news-base-recommender/dragonfly-local-limit-string"
"/ichwill-zen/production/dynproperties-ok-discovery-base-feeds/web-max-memory-used-by-snapshots-gigabytes-int"
)

PATHS_TO_SOURCE=(
"/ichwill-zen/production/bazinga_ok_social_recommender_worker/heartbeat"
)

echo "Starting parallel sync with $JOBS jobs..."

printf "%s\n" "${PATHS_TO_TARGET[@]}" | xargs -P "$JOBS" -I {} \
    java -jar "$ZKCOPY_JAR" \
    --source "$SOURCE{}" \
    --target "$TARGET{}" \
    --workers 10 \
    --copyOnly

printf "%s\n" "${PATHS_TO_SOURCE[@]}" | xargs -P "$JOBS" -I {} \
    java -jar "$ZKCOPY_JAR" \
    --source "$TARGET{}" \
    --target "$SOURCE{}" \
    --workers 10 \
    --copyOnly
