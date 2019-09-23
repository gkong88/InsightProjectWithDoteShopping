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
having count(*) > 1;

create table foo1 as 
select properties_shoppable_post_id, count(*) as views
from VIEWEDSHOPPABLEFIT_01_KEY_POSTID
window tumbling (size 30 second) 
group by properties_shoppable_post_id
having count(*) > 1;


select properties_shoppable_post_id, views from VIEWEDSHOPPABLEFIT_TABLE_most_view_30sec;


