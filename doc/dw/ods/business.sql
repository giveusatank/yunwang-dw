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


