CREATE TABLE `pv_count_user_all`
(
    `active_user`  string,
    `device_id`    string,
    `access_count` bigint,
    `product_id`   string,
    `company`      string,
    `country`      string,
    `province`     string,
    `city`         string,
    `location`     string
)
    partitioned by (date_day string)
    STORED AS parquet;

insert into pv_count_user_all PARTITION (date_day)
select active_user,
       device_id,
       count(1) as access_count,
       product_id,
       company,
       country,
       province,
       city,
       location,
       put_date
from action_do_log
where put_date like '201809%'
group by product_id, company, put_date, device_id, active_user, country, province, city, location;

