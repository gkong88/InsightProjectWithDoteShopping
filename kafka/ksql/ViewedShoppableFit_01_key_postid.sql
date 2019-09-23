DROP STREAM ViewedShoppableFit_01_key_postid;

CREATE STREAM foobar3
    (
        event VARCHAR,
        properties STRUCT <
            display VARCHAR,
            shoppable_post_id VARCHAR>
    )
    WITH (KAFKA_TOPIC = 'ViewedShoppableFit_00_raw_JSON',
          VALUE_FORMAT = 'JSON'
    );


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


DROP STREAM ViewedShoppableFit_02_key_postid_full_views_only;

CREATE STREAM ViewedShoppableFit_02_key_postid_full_views_only AS
 select * from ViewedShoppableFit_01_key_postid
 where PROPERTIES_DISPLAY = 'full view';
    
    
    DROP STREAM ViewedShoppableFit_03_key_postid_previews_only;

CREATE STREAM ViewedShoppableFit_03_key_postid_previews_only AS
 select * from ViewedShoppableFit_01_key_postid
 where PROPERTIES_DISPLAY = 'preview';
    
    
    CREATE STREAM recast2 AS
 select properties_display as disp, properties_shoppable_post_id as pid from ViewedShoppableFit_01_key_postid
 where properties_display = 'full view';