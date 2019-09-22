DROP STREAM ViewedShoppableFit_01_key_postid;

CREATE STREAM ViewedShoppableFit_01_key_postid 
    (
        event VARCHAR,
        originalTimestamp VARCHAR,
        properties_display VARCHAR,
        properties_shoppable_post_id VARCHAR, 
        properties_origin VARCHAR
    )
    WITH (KAFKA_TOPIC = 'ViewedShoppableFit_00_raw_flatJSON',
          VALUE_FORMAT = 'JSON',
          KEY = 'properties_shoppable_post_id');
