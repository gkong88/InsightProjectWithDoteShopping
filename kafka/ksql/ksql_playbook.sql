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

CREATE STREAM clicks_w_post_timestamp as
select CREATE_POST.segment_timestamp as segment_timestamp, CREATE_POST.properties_shoppable_post_id as properties_shoppable_post_id ,
        CLICK.properties_display as properties_display
        FROM CREATE_POST JOIN CLICK WITHIN 3 DAYS
        ON CREATE_POST.properties_shoppable_post_id = CLICK.properties_shoppable_post_id;


create table clicks_w_post_timestamp_day_counter AS
select post_id, segment_timestamp,
        count(*) as total,
        SUM(CASE WHEN properties_shoppable_post_id = 'preview' or properties_display = 'thumbnail'THEN 1 ELSE 0 END) as preview,
        SUM(CASE WHEN properties_shoppable_post_id = 'detail' or properties_display = 'full_view' THEN 1 ELSE 0 END) as full_view
        from clicks_w_post_timestamp
        WINDOW HOPPING (SIZE 3 DAYS, ADVANCE BY 1 minute )
        group by post_id, segment_timestamp;

CREATE STREAM clicks_w_post_timestamp_day_counter_report_stream
(
        post_id BIGINT,
        TOTAL BIGINT,
        PREVIEW INT,
        FULL_VIEW INT
    )
    WITH (KAFKA_TOPIC = 'clicks_w_post_timestamp_day_counter',
          VALUE_FORMAT = 'JSON',
          PARTITIONS = 1,
          REPLICAS = 1
          );




-- SCRATCH SHEET --
