CREATE STREAM trackViewedShoppableFit_01_stream
    (
        properties_display VARCHAR
    )
    WITH (KAFKA_TOPIC = 'trackViewedShoppableFit_00_raw_flatJSON',
          VALUE_FORMAT = 'JSON'
          );




CREATE TABLE trackViewedShoppableFit_02_previews_last_10min as 
select rowkey as shoppable_post_id, count(*) as previews
from trackViewedShoppableFit_01_stream
window hopping (size 10 minutes, advance by 2 minute)
where properties_display = 'preview'
group by rowkey;

CREATE TABLE trackViewedShoppableFit_02_fullviews_last_10min as 
select rowkey as shoppable_post_id, count(*) as fullviews
from trackViewedShoppableFit_01_stream
window hopping (size 10 minutes, advance by 2 minute)
where properties_display = 'detail'
group by rowkey;

CREATE TABLE trackViewedShoppableFit_03_previews_last_hour as 
select rowkey as shoppable_post_id, count(*) as previews
from trackViewedShoppableFit_01_stream
window hopping (size 60 minutes, advance by 10 minute)
where properties_display = 'preview'
group by rowkey;

CREATE TABLE trackViewedShoppableFit_03_fullviews_last_hour as 
select rowkey as shoppable_post_id, count(*) as fullviews
from trackViewedShoppableFit_01_stream
window hopping (size 60 minutes, advance by 10 minute)
where properties_display = 'detail'
group by rowkey;

CREATE TABLE trackViewedShoppableFit_04_previews_day as 
select rowkey as shoppable_post_id, count(*) as previews
from trackViewedShoppableFit_01_stream
window hopping (size 24 hours, advance by 6 hours)
where properties_display = 'preview'
group by rowkey;

CREATE TABLE trackViewedShoppableFit_04_fullviews_day as 
select rowkey as shoppable_post_id, count(*) as fullviews
from trackViewedShoppableFit_01_stream
window hopping (size 24 hours, advance by 6 hours)
where properties_display = 'detail'
group by rowkey;


CREATE TABLE hottest_posts_15_min4 as 
select event PROPERTIES_SHOPPABLE_POST_ID, count(*)*100000000 +  PROPERTIES_SHOPPABLE_POST_ID as count_post
from ViewedShoppableFit_01_key_postidKLUGE
window tumbling (size 15 minute) 
where properties_display = 'full view'
group by event, PROPERTIES_SHOPPABLE_POST_ID;


SELECT TOPK(count_post, 5) FROM hottest_posts3 WINDOW TUMBLING (size 15 minute) group by event;

