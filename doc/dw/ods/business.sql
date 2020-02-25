#/usr/local/apache-hive-1.2.2-bin/hcatalog/share/hcatalog/hive-hcatalog-core-1.2.2.jar copy to spark/jars

create external table if not exists ods.ods_product_user(
user_id            string,
product_id         string,
company            string,
reg_name           string,
nick_name          string,
real_name          string,
phone              string,
email              string,
sex                string,
birthday           string,
address            string,
org_id             string,
user_type          string,
first_access_time  string,
last_access_time   string,
last_access_ip     string,
country            string,
province           string,
city               string,
location           string,
row_timestamp      string,
row_status         string
)
partitioned by (put_date string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile location '/pep_cloud/business/ods/ods_product_user';

create external table if not exists ods.user_id(
user_id string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '~'
STORED AS TEXTFILE
LOCATION '/pep_cloud/business/ods/user_id';


CREATE TABLE `ods_order_detail`(`id` string COMMENT 'from deserializer', `app_id` string COMMENT 'from deserializer', `app_order_id` string COMMENT 'from deserializer', `product_id` string COMMENT 'from deserializer', `product_name` string COMMENT 'from deserializer', `price` string COMMENT 'from deserializer', `quantity` string COMMENT 'from deserializer', `type` string COMMENT 'from deserializer', `code` string COMMENT 'from deserializer', `start_time` string COMMENT 'from deserializer', `end_time` string COMMENT 'from deserializer', `beans` string COMMENT 'from deserializer', `materiel_code` string COMMENT 'from deserializer', `materiel_name` string COMMENT 'from deserializer', `row_timestamp` string COMMENT 'from deserializer', `row_status` string COMMENT 'from deserializer')
PARTITIONED BY (`count_date` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';



CREATE TABLE `ods_order_info`(`id` string COMMENT 'from deserializer', `app_id` string COMMENT 'from deserializer', `app_order_id` string COMMENT 'from deserializer', `user_id` string COMMENT 'from deserializer', `user_name` string COMMENT 'from deserializer', `sale_channel_id` string COMMENT 'from deserializer', `sale_channel_name` string COMMENT 'from deserializer', `s_state` string COMMENT 'from deserializer', `s_create_time` string COMMENT 'from deserializer', `s_delete_time` string COMMENT 'from deserializer', `order_price` string COMMENT 'from deserializer', `discount` string COMMENT 'from deserializer', `pay_channel` string COMMENT 'from deserializer', `pay_time` string COMMENT 'from deserializer', `pay_price` string COMMENT 'from deserializer', `pay_tradeno` string COMMENT 'from deserializer', `remark` string COMMENT 'from deserializer', `beans` string COMMENT 'from deserializer', `bean_type` string COMMENT 'from deserializer', `coupons` string COMMENT 'from deserializer', `row_timestamp` string COMMENT 'from deserializer', `row_status` string COMMENT 'from deserializer')
PARTITIONED BY (`count_date` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';