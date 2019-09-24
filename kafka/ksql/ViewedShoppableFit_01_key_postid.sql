CREATE STREAM ViewedShoppableFit_01_key_postidKLUGE2
    (
        event VARCHAR,
        timestamp VARCHAR,
        properties_display VARCHAR,
        properties_shoppable_post_id BIGINT, 
        properties_origin VARCHAR,
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



CREATE STREAM ViewedShoppableFit_01_key_postid_fullviewonly as 
    select * from ViewedShoppableFit_01_key_postid where properties_display = 'full view';




create table hottest_posts_15_min4 as 
select event, PROPERTIES_SHOPPABLE_POST_ID, count(*)*100000000 +  PROPERTIES_SHOPPABLE_POST_ID as count_post
from ViewedShoppableFit_01_key_postidKLUGE
window tumbling (size 15 minute) 
where properties_display = 'full view'
group by event, PROPERTIES_SHOPPABLE_POST_ID;


CREATE STREAM hottest_posts3
    (
        event VARCHAR,
        count_post BIGINT
    )
    WITH (KAFKA_TOPIC = 'HOTTEST_POSTS_15_MIN4',
          VALUE_FORMAT = 'JSON',
          KEY = 'event'
          );


SELECT TOPK(count_post, 5) FROM hottest_posts3 WINDOW TUMBLING (size 15 minute) group by event;

