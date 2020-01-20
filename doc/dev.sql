-- 订单用户的采集率
select substring(pay_time,1,6),app_id,count(distinct(a.user_id)),count(distinct(b.active_user))
from dwd_order_related_width a left join dwd_user_area b on a.user_id=b.active_user and a.app_id=b.product_id
and substring(pay_time,1,6)=from_unixtime(cast(substring(start_time, 1, 10) as bigint), 'yyyyMM') and a.app_id='1214'
group by substring(pay_time,1,6),app_id order by app_id,substring(pay_time,1,6);



select