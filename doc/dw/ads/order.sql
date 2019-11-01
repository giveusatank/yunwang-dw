create table if not exists ads_order_user_area_total(
product_id string,
company string,
country string,
provicne string,
sale_count string,
user_count string
) partitioned by (count_date string) stored as textfile;

create table if not exists ads_order_user_area_ym(
product_id string,
company string,
year_month string,
country string,
provicne string,
sale_count string,
user_count string
) partitioned by (count_date string) stored as textfile;

create table if not exists ads_order_user_zxxkc(
product_id string,
company string,
zxxkc string,
user_count string
) partitioned by (count_date string) stored as textfile;

create table if not exists ads_order_user_nj(
product_id string,
company string,
nj string,
user_count string
) partitioned by (count_date string) stored as textfile;

create table if not exists ads_order_user_total(
product_id string,
company string,
user_count string
) partitioned by (count_date string) stored as textfile;

create table if not exists ads_order_user_ym(
product_id string,
company string,
year_month string,
user_count string
) partitioned by (count_date string) stored as textfile;

create table if not exists ads_order_entity_total(
product_id string,
company string,
passive_obj string,
zxxkc string,
nj string,
user_count string,
sale_count string
) partitioned by (count_date string) stored as textfile;

create table if not exists ads_order_entity_ym(
product_id string,
company string,
year_month string,
passive_obj string,
zxxkc string,
nj string,
user_count string,
sale_count string
) partitioned by (count_date string) stored as textfile;