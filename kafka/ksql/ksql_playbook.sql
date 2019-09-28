
SET 'auto.offset.reset'='earliest';


-- create streams from topic

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


-- enrich posts w/ post creation date. filters out clicks on posts OUTSIDE of the scope of hotness/coldstarting function
CREATE STREAM CLICKS_POST_CREATE as
select CREATE_POST.segment_timestamp as segment_timestamp, CREATE_POST.properties_shoppable_post_id as properties_shoppable_post_id ,
        CLICK.properties_display as properties_display
        FROM CREATE_POST JOIN CLICK WITHIN 5 DAYS
        ON CREATE_POST.properties_shoppable_post_id = CLICK.properties_shoppable_post_id
        PARTITION BY properties_shoppable_post_id;


-- take "rolling" window summations of clicks
create table clicks_w_post_timestamp_agg AS
select properties_shoppable_post_id, segment_timestamp,
        count(*) as total,
        SUM(CASE WHEN properties_display = 'preview' or properties_display = 'thumbnail'THEN 1 ELSE 0 END) as preview,
        SUM(CASE WHEN properties_display = 'detail' or properties_display = 'full_view' THEN 1 ELSE 0 END) as full_view
        from CLICKS_POST_CREATE
        group by properties_shoppable_post_id, segment_timestamp;


create table clicks_w_post_timestamp_agg_w_score AS 
select properties_shoppable_post_id, PREVIEW, FULL_VIEW, CAST(FULL_VIEW as double) / (cast(PREVIEW as double) + cast(FULL_VIEW as double)), RTSCORING(CAST(preview AS BIGINT), CAST(full_view AS BIGINT)) 
from clicks_w_post_timestamp_agg;


