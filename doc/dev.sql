select t.product_id,t.company,c.cu as bus_reg,t.new_reg ,(c.cu-t.new_reg) as tou_reg,new_reg/c.cu as new_reg_ratio,(c.cu-t.new_reg)/d.tou_cu from (
select b.product_id,count(b.user_id) as new_reg,b.company from (
select active_user as active_user,product_id,company from dws.dws_uv_increase where nvl(active_user,'')!='' and count_date='20191028' group by active_user,product_id,company
) a join (
select user_id as user_id,product_id,company from dwd.dwd_product_user where
from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='20191028' group by user_id,product_id,company
) b on a.active_user=b.user_id and a.product_id=b.product_id  and a.company=b.company
group by b.product_id,b.company) t join (
select count(distinct(user_id)) as cu,product_id,company from dwd.dwd_product_user where
from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='20191028'  group by product_id,company
) c on t.product_id=c.product_id and t.company=c.company
join (
select tt.product_id,tt.company,count(1) tou_cu from (
select t.product_id,t.company,t.device_id from (
select a.product_id,a.company,a.active_user,a.device_id,nvl(b.device_id,'0000') as nvlid from
(select * from dws.dws_uv_daily where count_date='20191028')
a left join
(select * from dws.dws_uv_increase where count_date='20191028') b
on a.device_id=b.device_id and a.product_id=b.product_id and a.company=b.company and a.active_user=b.active_user and nvl(a.product_id,'')!='' and nvl(a.device_id,'')!=''
) t where t.nvlid='0000' group by t.product_id,t.company,t.device_id having nvl(max(t.active_user),'')=''
) tt group by tt.product_id,tt.company
) d on  t.product_id=d.product_id and t.company=d.company;

表a  当日新增用户中设备的first_access_time是今天的:即为新用户UserID。
表b  业务库中的新用户注册数  a和b join 出 新用户注册数 表t
表c  业务库中的新用户注册数  c和t join 出新增用户注册数占注册用户的比例，反集取出游客注册的比例
表d  每日访客中是游客的用户数  d和t join 出游客注册转化率

select t.product_id,t.company,c.cu as bus_reg,t.new_reg ,(c.cu-t.new_reg) as tou_reg,new_reg/c.cu as new_reg_ratio,(c.cu-t.new_reg)/d.tou_cu from (
select b.product_id,count(DISTINCT (b.user_id)) as new_reg,b.company from (-- 新用户注册数
select aa.product_id,aa.company,aa.active_user from dws.dws_uv_total aa join ( -- 采集注册用户中设备当日首次出现为新用户ID
select device_id,product_id,company from dws.dws_uv_increase where nvl(active_user,'')!='' and count_date='20191028' group by device_id,product_id,company -- 新用户表中的注册用户（新注册、游客注册）的设备ID
) bb on aa.device_id=bb.device_id and from_unixtime(cast(substring(aa.first_access_time, 1, 10) as bigint), 'yyyyMMdd')='20191028' and nvl(aa.active_user,'')!=''-- 筛选出当天出现的设备
) a join (
select user_id as user_id,product_id,company from dwd.dwd_product_user where -- 当天注册用户
from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='20191028' group by user_id,product_id,company
) b on a.active_user=b.user_id and a.product_id=b.product_id  and a.company=b.company
group by b.product_id,b.company) t join (
select count(distinct(user_id)) as cu,product_id,company from dwd.dwd_product_user where
from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='20191028'  group by product_id,company
) c on t.product_id=c.product_id and t.company=c.company
join (
select t.product_id,t.company,count(distinct(t.device_id)) as tou_cu from (
select a.product_id,a.company,a.device_id,nvl(b.device_id,'0000') as nvlid from (
select device_id,company,product_id from dws.dws_uv_daily where count_date='20191028' group by device_id,company,product_id having nvl(max(active_user),'')='' --游客+今日新用户
) a left join (
select device_id,company,product_id from dws.dws_uv_increase where count_date='20191028' group by device_id,company,product_id having nvl(max(active_user),'')='' --今日新用户
) b on a.company=b.company and a.device_id=b.device_id and a.product_id=b.product_id
) t where t.nvlid='0000' group by t.product_id,t.company
) d on  t.product_id=d.product_id and t.company=d.company;
















select t.product_id,t.company,count(1) from (
select a.product_id,a.company,a.device_id,nvl(b.device_id,'0000') as nvlid from (
select device_id,company,product_id from dws.dws_uv_daily where count_date='20191028' group by device_id,company,product_id having nvl(max(active_user),'')='' --游客+今日新用户
) a left join (
select device_id,company,product_id from dws.dws_uv_increase where count_date='20191028' group by device_id,company,product_id having nvl(max(active_user),'')='' --今日新用户
) b on a.company=b.company and a.device_id=b.device_id and a.product_id=b.product_id
) t where t.nvlid='0000' group by t.product_id,t.company