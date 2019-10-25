/* 
This document contains ksql commands to produce an event stream for real-time scoring
of recent posts.
*/

-- raw click event stream from segment
CREATE STREAM CLICK
    (
        properties_shoppable_post_id BIGINT,
        segment_timestamp BIGINT,
        ingest_timestamp BIGINT,
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
select CREATE_POST.segment_timestamp as post_timestamp, CLICK.segment_timestamp as click_timestamp, 
        CLICK.ingest_timestamp as ingest_timestamp, CREATE_POST.properties_shoppable_post_id as properties_shoppable_post_id ,
        CLICK.properties_display as properties_display
        FROM CREATE_POST LEFT JOIN CLICK WITHIN 36 HOURS
        ON CREATE_POST.properties_shoppable_post_id = CLICK.properties_shoppable_post_id
        PARTITION BY properties_shoppable_post_id;


-- click streams, (FI)ltered by recent posts, (AG)gregated by counts of post type
create table CLICK__FI_RECENT_POST__AG_COUNTS with (timestamp = 'post_timestamp') AS
select properties_shoppable_post_id, post_timestamp, 
        MAX(click_timestamp) as last_click_timestamp,
        MAX(ingest_timestamp) as ingest_timestamp,
        count(*) as total,
        SUM(CASE WHEN properties_display = 'preview' or properties_display = 'thumbnail' THEN 1 ELSE 0 END) as preview,
        SUM(CASE WHEN properties_display = 'detail' or properties_display = 'full_view' THEN 1 ELSE 0 END) as full_view
        from CLICK__FI_RECENT_POST
        group by properties_shoppable_post_id, post_timestamp;







