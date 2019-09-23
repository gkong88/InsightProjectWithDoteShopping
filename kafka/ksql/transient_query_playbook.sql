select * from VIEWEDSHOPPABLEFIT_01_KEY_POSTID;

select * from VIEWEDSHOPPABLEFIT_01_KEY_POSTID
where properties_display = 'full view';


drop table VIEWEDSHOPPABLEFIT_TABLE_most_view_30s;



create table views_top10_5minutes as 
select properties_shoppable_post_id, TOPK as views
from VIEWEDSHOPPABLEFIT_01_KEY_POSTID
window tumbling (size 3 minute) 
group by properties_shoppable_post_id
having views > 100;


create table VIEWEDSHOPPABLEFIT_TABLE_most_view_03m as 
select properties_shoppable_post_id, count(*) as views
from VIEWEDSHOPPABLEFIT_01_KEY_POSTID
window tumbling (size 3 minute) 
group by properties_shoppable_post_id
having count(*) > 10;

create table VIEWEDSHOPPABLEFIT_TABLE_most_view_30sec as 
select properties_shoppable_post_id, count(*) as views
from VIEWEDSHOPPABLEFIT_01_KEY_POSTID
window tumbling (size 30 second) 
group by properties_shoppable_post_id
having count(*) > 10;

create table foo2 as 
select properties_shoppable_post_id, count(*) as views
from VIEWEDSHOPPABLEFIT_01_KEY_POSTID
window tumbling (size 1 minute) 
group by properties_shoppable_post_id
having count(*) > 100;

create table foo3 as 
select properties_shoppable_post_id, count(*) as views
from VIEWEDSHOPPABLEFIT_01_KEY_POSTID
window tumbling (size 1 minute) 
group by properties_shoppable_post_id
having count(*) > 100;

create table foo4_full_view_counter_1minute as 
select properties_shoppable_post_id, count(*) as views
from ViewedShoppableFit_02_key_postid_full_views_only
window tumbling (size 1 minute) 
group by properties_shoppable_post_id;

create table foo4_full_view_counter_10s as 
select properties_shoppable_post_id, count(*) as views
from ViewedShoppableFit_02_key_postid_full_views_only
window tumbling (size 10 seconds) 
group by properties_shoppable_post_id;

create table asdf3 as 
select properties_shoppable_post_id
from VIEWEDSHOPPABLEFIT_01_KEY_POSTID
window tumbling (size 10 second) 
having count(*) > 10;




create stream foostream3 as select * from foo2;



select properties_shoppable_post_id, views from VIEWEDSHOPPABLEFIT_TABLE_most_view_30sec;


