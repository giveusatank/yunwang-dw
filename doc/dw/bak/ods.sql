[106.17.219.128, 100.116.192.177]~[1537285620.010]~1~1529936959038~~~1213~iPhone 6~iPhone OS~8.4.1~~~~教材打开,tape4b_002007~~~RapidCalculation.35781588~630e39943ece7caea83641aa933b9f08~dd100001~~~~~F95C709A-A272-4AA0-AAC9-B4ED29CB00A3~~

CREATE EXTERNAL TABLE IF NOT EXISTS original_action_log
(
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
    LOCATION '/pep_cloud/ulog/ods/original_action_log';

drop table action_log;

CREATE EXTERNAL TABLE IF NOT EXISTS action_log_ot
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
    LOCATION '/pep_cloud/ulog/ods/action_log_ot';


CREATE EXTERNAL TABLE IF NOT EXISTS action_log_p
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
    STORED AS parquet
    LOCATION '/pep_cloud/ulog/ods/action_log';

select count(1)
from original_action_log
where log_version != ''
  and from_unixtime(cast(substring(start_time, 1, 10) as bigint), 'yyyyMMdd') = '20180918';

报错：
FAILED: SemanticException [Error 10096]: Dynamic partition strict mode requires at least one static partition column.
To turn this off
set hive.exec.dynamic.partition.mode=nonstrict

解决方案：（spark 2.0需要在hive-site.xml中添加配置）
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;


insert into action_log PARTITION (put_date)
select '',
       remote_addr,
       request_time,
       log_version,
       start_time,
       end_time,
       region,
       product_id,
       hardware,
       os,
       soft,
       active_user,
       active_org,
       active_type,
       passive_obj,
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
       from_unixtime(cast(substring(start_time, 1, 10) as bigint), 'yyyyMMdd') as put_date
from original_action_log_p
where put_date = '20180703'
  and start_time is not null
  and from_unixtime(cast(substring(start_time, 1, 10) as bigint), 'yyyyMMdd') in
      ('20180701', '20180702', '20180703') distribute by substring(start_time, 8, 10);



select count(1)
from action_log
where put_date = '20180918';
