HDFS 外部表 建表SQL 11

DROP TABLE IF EXISTS `action_log`;

CREATE EXTERNAL TABLE IF NOT EXISTS action_log
(
    id            STRING,
    remote_addr   STRING,
    request_time  STRING,
    log_version   STRING,
    start_time    bigint,
    end_time      bigint,
    region        STRING,
    product_id    STRING,
    hardware      STRING,
    os            STRING,
    soft          STRING,
    active_user   STRING,
    active_org    STRING,
    active_type   int,
    passive_obj   STRING,
    passive_type  STRING,
    from_prod     STRING,
    from_pos      STRING,
    company       string,
    action_title  STRING,
    action_type   int,
    request       STRING,
    request_param STRING,
    group_type    int,
    group_id      STRING,
    result_flag   int,
    result        STRING
)
    partitioned by (put_date int)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~'
    STORED AS TEXTFILE
    LOCATION '/logs/action_log';

格式化输出日志

select str_to_map(
               concat(
                       'id=', cast(id as string),
                       '~remote_addr=', cast(remote_addr as string),
                       '~request_time=', cast(request_time as string),
                       '~log_version=', cast(log_version as string),
                       '~start_time=', cast(start_time as string),
                       '~start_time=', cast(start_time as string),
                       '~region=', cast(nvl(region, '') as string),
                       '~product_id=', cast(nvl(product_id, '') as string),
                       '~hardware=', cast(nvl(hardware, '') as string),
                       '~os=', cast(nvl(os, '') as string),
                       '~soft=', cast(nvl(soft, '') as string),
                       '~active_user=', cast(nvl(active_user, '') as string),
                       '~active_org=', cast(nvl(active_org, '') as string),
                       '~active_type=', cast(nvl(active_type, '') as string),
                       '~passive_obj=', cast(nvl(passive_obj, '') as string),
                       '~passive_type=', cast(nvl(passive_type, '') as string),
                       '~from_prod=', cast(nvl(from_prod, '') as string),
                       '~from_pos=', cast(nvl(from_pos, '') as string),
                       '~company=', cast(nvl(company, '') as string),
                       '~action_title=', cast(nvl(action_title, '') as string),
                       '~action_type=', cast(nvl(action_type, '') as string),
                       '~request=', cast(nvl(request, '') as string),
                       '~request_param=', cast(nvl(request_param, '') as string),
                       '~group_type=', cast(nvl(group_type, '') as string),
                       '~group_id=', cast(nvl(group_id, '') as string),
                       '~result_flag=', cast(nvl(result_flag, '') as string),
                       '~result=', cast(nvl(result, '') as string),
                       '~put_date=', cast(nvl(put_date, '') as string)
                   )
           , '~', '=')
from action_log
where put_date = '201809'
  and action_title = 'dd100001'
  and product_id = '11120101';



select str_to_map(
               concat(
                       'id=', cast(id as string),
                       '~remote_addr=', cast(remote_addr as string),
                       '~request_time=', cast(request_time as string),
                       '~log_version=', cast(log_version as string),
                       '~start_time=', cast(start_time as string),
                       '~start_time=', cast(start_time as string),
                       '~active_user=', cast(nvl(active_user, '') as string),
                       '~device_id=', cast(nvl(device_id, '') as string),
                       '~active_org=', cast(nvl(active_org, '') as string),
                       '~active_type=', cast(nvl(active_type, '') as string),
                       '~passive_obj=', cast(nvl(passive_obj, '') as string),
                       '~passive_type=', cast(nvl(passive_type, '') as string),
                       '~from_prod=', cast(nvl(from_prod, '') as string),
                       '~from_pos=', cast(nvl(from_pos, '') as string),
                       '~company=', cast(nvl(company, '') as string),
                       '~action_title=', cast(nvl(action_title, '') as string),
                       '~action_type=', cast(nvl(action_type, '') as string),
                       '~request=', cast(nvl(request, '') as string),
                       '~request_param=', cast(nvl(request_param, '') as string),
                       '~group_type=', cast(nvl(group_type, '') as string),
                       '~group_id=', cast(nvl(group_id, '') as string),
                       '~result_flag=', cast(nvl(result_flag, '') as string),
                       '~result=', cast(nvl(result, '') as string),
                       '~put_date=', cast(nvl(put_date, '') as string)
                   )
           , '~', '=')
from log_180918
limit 100
    清洗日志数据（去重版）

CREATE TABLE `log_180918_1`
(
    `id`            string,
    `remote_addr`   string,
    `country`       string,
    `province`      string,
    `city`          string,
    `location`      string,
    `request_time`  string,
    `log_version`   string,
    `start_time`    bigint,
    `end_time`      bigint,
    `region`        string,
    `product_id`    string,
    `os`            string,
    `hardware`      string,
    `device_id`     string,
    `active_user`   string,
    `active_org`    string,
    `active_type`   int,
    `passive_obj`   string,
    `passive_type`  string,
    `from_prod`     string,
    `from_pos`      string,
    `company`       string,
    `action_title`  string,
    `action_type`   int,
    `request`       string,
    `request_param` string,
    `group_type`    int,
    `group_id`      string,
    `result_flag`   int,
    `result`        string,
    `put_date`      int,
    `num`           int
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~';

insert into action_do_log_text
select *
from (select *, row_number() over (partition by group_id,start_time order by start_time,group_id) num
      from (
               select id,
                      remote_addr,
                      split(ip2region(regexp_extract(remote_addr, '(?<=(\\[))\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}',
                                                     0)), '~')[0]                               as country,
                      split(ip2region(regexp_extract(remote_addr, '(?<=(\\[))\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}',
                                                     0)), '~')[1]                               as province,
                      split(ip2region(regexp_extract(remote_addr, '(?<=(\\[))\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}',
                                                     0)), '~')[2]                               as city,
                      split(ip2region(regexp_extract(remote_addr, '(?<=(\\[))\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}',
                                                     0)), '~')[3]                               as loaction,
                      request_time,
                      log_version,
                      start_time,
                      end_time,
                      region,
                      product_id,
                      if(log_version > 1, os, null)                                                os,
                      if(log_version > 1, hardware, null)                                          hardware,
                      if(log_version > 1, str_to_map(os, ',', ':')['deviceId'],
                         if(length(regexp_extract(os, '(?<=(DeviceId\\(IMEI\\)\\:))\\d+', 0)) != 0,
                            regexp_extract(os, '(?<=(DeviceId\\(IMEI\\)\\:))\\d+', 0),
                            if(length(group_id) != 13, group_id, '')))                          as device_id,
                      active_user,
                      active_org,
                      active_type,
                      if(locate(',', passive_obj) > 0, split(passive_obj, ',')[1], passive_obj) as passive_obj,
                      passive_type,
                      from_prod,
                      from_pos,
                      company,
                      action_title,
                      action_type,
                      request,
                      request_param,
                      group_type,
                      group_id,
                      result_flag,
                      result,
                      put_date
               from action_log
               where put_date = '20180918'
           ) tmp
     ) t
where t.num = 1
limit 100;


select ip2region('219.239.238.48', 0);

select if(length(regexp_extract(os, '(?<=(DeviceId\\(IMEI\\)\\:))\\d+', 0)) != 0,
          regexp_extract(os, '(?<=(DeviceId\\(IMEI\\)\\:))\\d+', 0),
          if(length(group_id) != 13, group_id, '')) as device_id
from action_log
where put_date in ('20180918')
limit 100;

OdsActionDoLogCleaner

CREATE TABLE `action_do_log`
(
    `id`            string,
    `remote_addr`   string,
    `country`       string,
    `province`      string,
    `city`          string,
    `location`      string,
    `request_time`  string,
    `log_version`   string,
    `start_time`    bigint,
    `end_time`      bigint,
    `region`        string,
    `product_id`    string,
    `os`            string,
    `hardware`      string,
    `device_id`     string,
    `active_user`   string,
    `active_org`    string,
    `active_type`   int,
    `passive_obj`   string,
    `passive_type`  string,
    `from_prod`     string,
    `from_pos`      string,
    `company`       string,
    `action_title`  string,
    `action_type`   int,
    `request`       string,
    `request_param` string,
    `group_type`    int,
    `group_id`      string,
    `result_flag`   int,
    `result`        string,
    `num`           int
)
    partitioned by (put_date int)
    STORED AS parquet;

insert into action_do_log_text PARTITION (put_date)
select *
from (select *, row_number() over (partition by group_id,start_time order by start_time,group_id) num, '20180916'
      from (
               select id,
                      remote_addr,
                      split(ip2region(regexp_extract(remote_addr, '(?<=(\\[))\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}',
                                                     0)), '~')[0]                               as country,
                      split(ip2region(regexp_extract(remote_addr, '(?<=(\\[))\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}',
                                                     0)), '~')[1]                               as province,
                      split(ip2region(regexp_extract(remote_addr, '(?<=(\\[))\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}',
                                                     0)), '~')[2]                               as city,
                      split(ip2region(regexp_extract(remote_addr, '(?<=(\\[))\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}',
                                                     0)), '~')[3]                               as loaction,
                      request_time,
                      log_version,
                      start_time,
                      end_time,
                      region,
                      product_id,
                      if(log_version > 1, os, null)                                                os,
                      if(log_version > 1, hardware, null)                                          hardware,
                      if(log_version > 1, str_to_map(os, ',', ':')['deviceId'],
                         if(length(regexp_extract(os, '(?<=(DeviceId\\(IMEI\\)\\:))\\d+', 0)) != 0,
                            regexp_extract(os, '(?<=(DeviceId\\(IMEI\\)\\:))\\d+', 0),
                            if(length(group_id) != 13, group_id, '')))                          as device_id,
                      active_user,
                      active_org,
                      active_type,
                      if(locate(',', passive_obj) > 0, split(passive_obj, ',')[1], passive_obj) as passive_obj,
                      passive_type,
                      from_prod,
                      from_pos,
                      company,
                      action_title,
                      action_type,
                      request,
                      request_param,
                      group_type,
                      group_id,
                      result_flag,
                      result
               from action_log
               where put_date = '20180916'
           ) tmp
     ) t
where t.num = 1

CREATE TABLE `action_do_log_text`
(
    `id`            string,
    `remote_addr`   string,
    `country`       string,
    `province`      string,
    `city`          string,
    `location`      string,
    `request_time`  string,
    `log_version`   string,
    `start_time`    bigint,
    `end_time`      bigint,
    `region`        string,
    `product_id`    string,
    `os`            string,
    `hardware`      string,
    `device_id`     string,
    `active_user`   string,
    `active_org`    string,
    `active_type`   int,
    `passive_obj`   string,
    `passive_type`  string,
    `from_prod`     string,
    `from_pos`      string,
    `company`       string,
    `action_title`  string,
    `action_type`   int,
    `request`       string,
    `request_param` string,
    `group_type`    int,
    `group_id`      string,
    `result_flag`   int,
    `result`        string,
    `num`           int
)
    partitioned by (put_date int)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~'
    STORED AS TEXTFILE;


按时间、访问次数统计

DROP TABLE IF EXISTS `pv_count_date_180918`;

CREATE TABLE `pv_count_date_180918`
(
    `user_count`   bigint,
    `count_team`   string,
    `access_count` bigint,
    `product_id`   string,
    `company`      string,
    `date_hour`    string,
    `split_type`   string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~';

###### 按小时统计
insert into pv_count_date_180918
Select count(1) user_count, count_team, sum(access_count) as access_count, product_id, company, date_hour, 'h'
from (
         SELECT CASE
                    WHEN access_count < 2 THEN 'A'
                    WHEN access_count < 5 THEN 'B'
                    WHEN access_count < 8 THEN 'C'
                    WHEN access_count < 10 THEN 'D'
                    WHEN access_count < 20 THEN 'E'
                    WHEN access_count < 40 THEN 'F'
                    WHEN access_count < 60 THEN 'G'
                    WHEN access_count < 80 THEN 'H'
                    else 'I' end AS count_team,
                product_id,
                company,
                device_id,
                date_hour,
                access_count
         from (
                  select count(1)                                                                     as access_count,
                         product_id,
                         company,
                         from_unixtime(cast(substring(start_time, 1, 10) as bigint), 'yyyy-MM-dd HH') as date_hour,
                         device_id
                  from log_180918
                  group by product_id, company,
                           from_unixtime(cast(substring(start_time, 1, 10) as bigint), 'yyyy-MM-dd HH'), device_id
              ) tmp
     ) tmp1
group by product_id, company, date_hour, count_team;


###### 按天统计
insert into pv_count_date_180918
Select count(1) user_count, count_team, sum(access_count) as access_count, product_id, company, date_day, 'd'
from (
         SELECT CASE
                    WHEN access_count < 2 THEN 'A'
                    WHEN access_count < 5 THEN 'B'
                    WHEN access_count < 8 THEN 'C'
                    WHEN access_count < 10 THEN 'D'
                    WHEN access_count < 20 THEN 'E'
                    WHEN access_count < 40 THEN 'F'
                    WHEN access_count < 60 THEN 'G'
                    WHEN access_count < 80 THEN 'H'
                    else 'I' end AS count_team,
                product_id,
                company,
                device_id,
                date_day,
                access_count
         from (
                  select count(1)                                                                  as access_count,
                         product_id,
                         company,
                         from_unixtime(cast(substring(start_time, 1, 10) as bigint), 'yyyy-MM-dd') as date_day,
                         device_id
                  from log_180918
                  group by product_id, company,
                           from_unixtime(cast(substring(start_time, 1, 10) as bigint), 'yyyy-MM-dd'), device_id
              ) tmp
     ) tmp1
group by product_id, company, date_day, count_team;

sqoop export --connect jdbc:mysql://192.168.186.36:3306/bi --username root --password 123456  --table pv_count_date   --export-dir "/hive/warehouse/ods.db/pv_count_date_180918" --fields-terminated-by '~' --columns "user_count,count_team,access_count,product_id,company,date_str,split_type"


PV统计 用户表  累计每天用户的访问次数 如同"log_"开头的表不轻易删除   里面的内容会按月切分成备份表和查询表

#
DROP TABLE IF EXISTS `pv_count_user_all`;

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
    `location`     string,
    `date_day`     string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~';

#
insert into pv_count_user_all
select *
from pv_count_user;

insert into pv_count_user_all
select active_user,
       device_id,
       count(1)                                                                  as access_count,
       product_id,
       company,
       country,
       province,
       city,
       location,
       from_unixtime(cast(substring(start_time, 1, 10) as bigint), 'yyyy-MM-dd') as date_day
from log_180918
group by product_id, company, from_unixtime(cast(substring(start_time, 1, 10) as bigint), 'yyyy-MM-dd'), device_id,
         active_user, country, province, city, location;

###访问用户去重
DROP TABLE IF EXISTS `pv_count_user`;

CREATE TABLE `pv_count_user`
(
    `active_user`  string,
    `device_id`    string,
    `access_count` bigint,
    `product_id`   string,
    `company`      string,
    `country`      string,
    `province`     string,
    `city`         string,
    `location`     string,
    `date_day`     string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~';

insert into pv_count_user
select active_user,
       device_id,
       sum(access_count)                as access_count,
       product_id,
       company,
       country,
       province,
       city,
       location,
       date_format(date_day, "yyyy-MM") as date_day
from pv_count_user_all
group by product_id, company, date_format(date_day, "yyyy-MM"), device_id, active_user, country, province, city,
         location;



###### 按周统计 按周执行

select weekofyear(date_format('2018-09-18', "yyyy-MM-dd"));

DROP TABLE IF EXISTS `pv_count_week_2018_38`;

CREATE TABLE `pv_count_week_2018_38`
(
    `user_count`   bigint,
    `count_team`   string,
    `access_count` bigint,
    `product_id`   string,
    `company`      string,
    `date_str`     string,
    `split_type`   string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~';

insert into pv_count_week_2018_38
Select count(1) user_count, count_team, sum(access_count) as access_count, product_id, company, date_str, 'w'
from (
         SELECT CASE
                    WHEN access_count < 2 THEN 'A'
                    WHEN access_count < 5 THEN 'B'
                    WHEN access_count < 8 THEN 'C'
                    WHEN access_count < 10 THEN 'D'
                    WHEN access_count < 20 THEN 'E'
                    WHEN access_count < 40 THEN 'F'
                    WHEN access_count < 60 THEN 'G'
                    WHEN access_count < 80 THEN 'H'
                    else 'I' end                                                                        AS count_team,
                product_id,
                company,
                device_id,
                concat(substring(date_day, 1, 4), ' ', weekofyear(date_format(date_day, "yyyy-MM-dd"))) as date_str,
                access_count
         from pv_count_user
         where concat(substring(date_day, 1, 4), ' ', weekofyear(date_format(date_day, "yyyy-MM-dd"))) = '2018 38'
     ) tmp1
group by product_id, company, date_str, count_team;

sqoop export --connect jdbc:mysql://192.168.186.36:3306/bi --username root --password 123456  --table pv_count_date   --export-dir "/hive/warehouse/ods.db/pv_count_week_2018_38" --fields-terminated-by '~' --columns "user_count,count_team,access_count,product_id,company,date_str,split_type"

###### 按月统计 按月执行

DROP TABLE IF EXISTS `pv_count_month_201809`;

CREATE TABLE `pv_count_month_201809`
(
    `user_count`   bigint,
    `count_team`   string,
    `access_count` bigint,
    `product_id`   string,
    `company`      string,
    `date_str`     string,
    `split_type`   string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~';

insert into pv_count_month_201809
Select count(1) user_count, count_team, sum(access_count) as access_count, product_id, company, date_str, 'm'
from (
         SELECT CASE
                    WHEN access_count < 2 THEN 'A'
                    WHEN access_count < 5 THEN 'B'
                    WHEN access_count < 8 THEN 'C'
                    WHEN access_count < 10 THEN 'D'
                    WHEN access_count < 20 THEN 'E'
                    WHEN access_count < 40 THEN 'F'
                    WHEN access_count < 60 THEN 'G'
                    WHEN access_count < 80 THEN 'H'
                    else 'I' end          AS count_team,
                product_id,
                company,
                device_id,
                substring(date_day, 1, 7) as date_str,
                access_count
         from pv_count_user
         where substring(date_day, 1, 7) = '2018-09'
     ) tmp1
group by product_id, company, date_str, count_team;



sqoop export --connect jdbc:mysql://192.168.186.36:3306/bi --username root --password 123456  --table pv_count_date   --export-dir "/hive/warehouse/ods.db/pv_count_month_201809" --fields-terminated-by '~' --columns "user_count,count_team,access_count,product_id,company,date_str,split_type"


全国用户分布

DROP TABLE IF EXISTS `ip_count_1809`;

CREATE TABLE `ip_count_1809`
(
    `product_id` string,
    `company`    string,
    `country`    string,
    `province`   string,
    `city`       string,
    `location`   string,
    `usercount`  bigint,
    `date_day`   string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~';

insert into ip_count_1809
select product_id,
       company,
       country,
       province,
       city,
       location,
       sum(1),
       date_day
from pv_count_user
where product_id != ''
  and country = '中国'
group by product_id, company, country, province, city, location, date_day;

sqoop export --connect jdbc:mysql://192.168.186.36:3306/bi --username root --password 123456  --table ip_count   --export-dir "/hive/warehouse/ods.db/ip_count_1809" --fields-terminated-by '~' --columns "product_id,company,country,province,city,location,usercount,date_day";


教材基础统计:{日期_小时 产品 渠道 地理位置 用户 教材}

DROP TABLE IF EXISTS `open_action_180918`;

CREATE TABLE `open_action_180918`
(
    `country`            string,
    `province`           string,
    `city`               string,
    `location`           string,
    `product_id`         string,
    `company`            string,
    `group_id`           string,
    `passive_obj`        string,
    `sum_time_consume`   bigint,
    `avg_time_consume`   bigint,
    `start_action`       string,
    `start_action_count` bigint,
    `action_count`       bigint,
    `date_hour`          string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~';

insert into open_action_180918
select country,
       province,
       city,
       location,
       product_id,
       company,
       group_id,
       passive_obj,
       TimeConsume(str_to_map(concat_ws(",", collect_set(
               concat_ws(':', cast(start_time as string), cast(action_title as string))))), "dd100001,jx200001",
                   "dd100002,jx200002", 0)                                                                        as sum_time_consume,
       TimeConsume(str_to_map(concat_ws(",", collect_set(
               concat_ws(':', cast(start_time as string), cast(action_title as string))))), "dd100001,jx200001",
                   "dd100002,jx200002",
                   1)                                                                                             as avg_time_consume,
       if(action_title in ('dd100001', 'jx200001'), action_title, case
                                                                      when action_title = 'dd100002' then 'dd100001'
                                                                      when action_title = 'jx200002'
                                                                          then 'jx200001' end)                    as start_action,
       sum(if(action_title in ('dd100001', 'jx200001'), 1, 0))                                                    as start_action_count,
       count(1)                                                                                                   as action_count,
       from_unixtime(cast(substring(start_time, 1, 10) as bigint),
                     'yyyy-MM-dd HH')                                                                             as date_hour
from action_do_log
where action_title in ('dd100001', 'dd100002', 'jx200001', 'jx200002')
  and group_id != ''
  and put_date = '20190426'
group by product_id, company, country, province, city, location, group_id, passive_obj,
         from_unixtime(cast(substring(start_time, 1, 10) as bigint), 'yyyy-MM-dd HH'),
         if(action_title in ('dd100001', 'jx200001'), action_title,
            case when action_title = 'dd100002' then 'dd100001' when action_title = 'jx200002' then 'jx200001' end);


教材使用情况:{日期_天 产品 渠道 教材} [教材打开事件] (总数 平均学习时长 学习总时长)

DROP TABLE IF EXISTS `action_open_count_180918`;

CREATE TABLE `action_open_count_180918`
(
    `product_id`            string,
    `company`               string,
    `passive_obj`           string,
    `sum_time_consume`      bigint,
    `avg_time_consume`      bigint,
    `simulate_time_consume` bigint,
    `start_action`          string,
    `start_action_count`    bigint,
    `action_count`          bigint,
    `date_day`              string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~';

insert into action_open_count_180918
select product_id,
       company,
       passive_obj,
       sum(sum_time_consume)                                                           as sum_time_consume,
       if(sum(start_action_count) == 0, 0,
          round(sum(avg_time_consume * start_action_count) / sum(start_action_count))) as avg_time_consume,
       sum(avg_time_consume * start_action_count)                                      as simulate_time_consume,
       start_action,
       sum(start_action_count)                                                         as start_action_count,
       sum(action_count)                                                               as action_count,
       substring(date_hour, 1, 10)                                                     as date_day
from open_action_180918
group by product_id, company, passive_obj, substring(date_hour, 1, 10), start_action;

sqoop export --connect jdbc:mysql://192.168.186.36:3306/bi --username root --password 123456 --table action_open_count --export-dir "/hive/warehouse/ods.db/action_open_count_180918" --fields-terminated-by '~' --columns "product_id,company,tb_id,sum_time_consume,avg_time_consume,simulate_time_consume,start_action,start_action_count,action_count,date_day";



教材地区用户量:{每天 每个产品 每个地区}[打开教材事件](用户统计)

DROP TABLE IF EXISTS `action_open_cp_180918`;

CREATE TABLE `action_open_cp_180918`
(
    `product_id`         string,
    `company`            string,
    `country`            string,
    `province`           string,
    `city`               string,
    `location`           string,
    `user_count`         bigint,
    `object_open_count`  bigint,
    `sum_time_consume`   bigint,
    `avg_time_consume`   bigint,
    `start_action_count` bigint,
    `start_action`       string,
    `action_count`       bigint,
    `date_day`           string
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '~';

insert into action_open_cp_180918
select product_id,
       company,
       country,
       province,
       city,
       location,
       count(group_id)                                                                 as user_count,
       count(passive_obj)                                                              as textbook_open_count,
       sum(sum_time_consume)                                                           as sum_time_consume,
       if(sum(start_action_count) == 0, 0,
          round(sum(avg_time_consume * start_action_count) / sum(start_action_count))) as avg_time_consume,
       count(start_action_count)                                                       as start_action_count,
       start_action,
       sum(action_count)                                                               as action_count,
       substring(date_hour, 1, 10)                                                     as date_day
from open_action_180918
group by product_id, start_action, company, country, province, city, location, substring(date_hour, 1, 10);

sqoop export --connect jdbc:mysql://192.168.186.36:3306/bi --username root --password 123456 --table action_open_ip_count --export-dir "/hive/warehouse/ods.db/action_open_cp_180918" --fields-terminated-by '~' --columns "product_id,company,country,province,city,location,user_count,object_open_count,sum_time_consume,avg_time_consume,start_action_count,start_action,action_count,date_day";