/* 
This document contains ksql commands to produce an event stream for real-time scoring
of recent posts.
*/

SET 'auto.offset.reset'='earliest';

-- raw click event stream from segment
CREATE STREAM CLICK
    (
        properties_shoppable_post_id BIGINT,
        segment_timestamp BIGINT,
        properties_display VARCHAR
    )
    WITH (KAFKA_TOPIC = 'trackViewedShoppableFit_00_raw_flatJSON',
          VALUE_FORMAT = 'JSON',
          KEY='properties_shoppable_post_id',
          TIMESTAMP ='segment_timestamp'
          );

-- raw post creation event stream from segment
-- DEVELOPER NOTE: Check to ensure that the source kafka topic has a retention time of at least 3 days!
CREATE STREAM CREATE_POST
    (
        properties_shoppable_post_id BIGINT,
        segment_timestamp BIGINT
    )
    WITH (KAFKA_TOPIC = 'trackCreatedStory_00_raw_flatJSON',
          VALUE_FORMAT = 'JSON',
          KEY='properties_shoppable_post_id',
          TIMESTAMP ='segment_timestamp'
          );

-- click streams (FI)ltered by recent posts, enriched by post creation timestamp
CREATE STREAM CLICK__FI_RECENT_POST with (timestamp = 'post_timestamp') as
select CREATE_POST.segment_timestamp as post_timestamp, CLICK.segment_timestamp as click_timestamp, CREATE_POST.properties_shoppable_post_id as properties_shoppable_post_id ,
        CLICK.properties_display as properties_display
        FROM CREATE_POST LEFT JOIN CLICK WITHIN 36 HOURS
        ON CREATE_POST.properties_shoppable_post_id = CLICK.properties_shoppable_post_id
        PARTITION BY properties_shoppable_post_id;


-- click streams, (FI)ltered by recent posts, (AG)gregated by counts of post type
create table CLICK__FI_RECENT_POST__AG_COUNTS with (timestamp = 'post_timestamp') AS
select properties_shoppable_post_id, post_timestamp, 
        MAX(click_timestamp) as last_click_timestamp,
        count(*) as total,
        SUM(CASE WHEN properties_display = 'preview' or properties_display = 'thumbnail' THEN 1 ELSE 0 END) as preview,
        SUM(CASE WHEN properties_display = 'detail' or properties_display = 'full_view' THEN 1 ELSE 0 END) as full_view
        from CLICK__FI_RECENT_POST
        group by properties_shoppable_post_id, post_timestamp;

-- click streams, (FI)iltered by recent posts, (AG)gregated by counts of post type, (EN)riched with a custom scoring function
-- DEVELOPER NOTE: Overwriting old data: Check to ensure the kafka topic has LOG COMPACTION ENABLED. This greatly reduces
--                     the computational burden when materializing a snapshot of this table via replay
-- DEVELOPER NOTE: Garbage collection: Check to ensure the kafka topic has LOG DELETION ENABLED and set the log retention time
--                      to that of the appropriate time range.
--                      This provides a "garbage collection"mechanism to retire old data from the topic and eases
--                      storage burden on kafka, as well as computational burden for materializing a snapshot.
-- DEVELOPER NOTE: see ksql UDF docs to change scoring function: https://docs.confluent.io/current/ksql/docs/developer-guide/udf.html
-- DEVELOPER NOTE: Check to ensure that the source kafka topic has a retention time of at least 3 days!
create table CLICK__FI_RECENT_POST__AG_COUNTS__EN_SCORE2 with (PARTITIONS = 1) AS 
select properties_shoppable_post_id, post_timestamp, last_click_timestamp, PREVIEW, FULL_VIEW, (CAST(FULL_VIEW as double) / (cast(PREVIEW as double) + cast(FULL_VIEW as double))) as CTR, RTSCORING(CAST(preview AS BIGINT), CAST(full_view AS BIGINT)) as RT_SCORE
from CLICK__FI_RECENT_POST__AG_COUNTS;







