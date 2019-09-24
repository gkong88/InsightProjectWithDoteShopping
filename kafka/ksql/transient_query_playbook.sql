




select rowkey, topk(fullview, 5) from count_full_views_last_3minute group by rowkey;


create table count_full_views_last_3minute as 
select rowkey, count(*) as fullview
from ViewedShoppableFit_01_key_postid_fullviewonly
window tumbling (size 3 minute) 
group by rowkey;


create table count_full_views_last_15minute as 
select rowkey as post_id , count(*) as fullview
from ViewedShoppableFit_01_key_postid_fullviewonly
window tumbling (size 15 minute) 
group by rowkey;
