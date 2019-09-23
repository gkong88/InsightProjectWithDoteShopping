DROP STREAM ViewedShoppableFit_01_key_postid;

CREATE STREAM ViewedShoppableFit_01_key_postid 
    (
        event VARCHAR,
        timestamp VARCHAR,
        properties_display VARCHAR,
        properties_shoppable_post_id VARCHAR, 
        properties_origin VARCHAR
    )
    WITH (KAFKA_TOPIC = 'ViewedShoppableFit_00_raw_flatJSON',
          VALUE_FORMAT = 'JSON',
          KEY = 'properties_shoppable_post_id'
          /* 
          timestamp = 'timestamp',
                            "2019-09-21T20:31:07.942Z"
           TIMESTAMP_FORMAT = 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''
            */
          );
