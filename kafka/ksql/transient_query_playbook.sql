


create table count_full_views_last_15minuteKLUGE as 
select PROPERTIES_SHOPPABLE_POST_ID, count(*)*100000000 +  PROPERTIES_SHOPPABLE_POST_ID as count_post
from ViewedShoppableFit_01_key_postidKLUGE
window tumbling (size 15 minute) 
where properties_display = 'full view'
group by PROPERTIES_SHOPPABLE_POST_ID;

select 


create table count_full_views_last_15minute as 
select rowkey as post_id , count(*) as fullview
from ViewedShoppableFit_01_key_postid_fullviewonly
window tumbling (size 15 minute) 
group by rowkey;

select