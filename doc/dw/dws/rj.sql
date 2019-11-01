#每天dpi使用情况分组统计
create table if not exists dws.dws_dpi_daily
(
    product_id string,
    company    string,
    dpi_name   string,
    dpi_count  bigint
) partitioned by (count_date bigint)  stored as parquet

#每天浏览器使用情况分组统计
create table if not exists dws.dws_browser_daily
(
    product_id    string,
    company       string,
    browser_name  string,
    browser_count bigint
) partitioned by (count_date bigint)  stored as parquet

#每天独立用户访问次数分布
create table if not exists dws.dws_access_distrib_daily
(
    product_id               string,
    company                  string,
    country                  string,
    province                 string,
    city                     string,
    sess_per_user_zone       string,
    sess_per_user_zone_count bigint
) partitioned by (count_date bigint) stored as parquet

#每天每次访问浏览数分布
create table if not exists dws.dws_action_distrib_daily
(
    product_id              string,
    company                 string,
    country                 string,
    province                string,
    city                    string,
    act_per_sess_zone       string,
    act_per_sess_zone_count bigint
) partitioned by (count_date bigint) stored as parquet

#每天独立ip分组统计
create table if not exists dws.dws_ip_daily
(
    product_id string,
    company    string,
    country    string,
    province   string,
    city       string,
    ip_count   bigint
) partitioned by (count_date bigint) stored as parquet

#每天人均访问次数 人均浏览次数 平均每次浏览数 分组统计
create table if not exists dws.dws_per_daily
(
    product_id    string,
    company       string,
    country       string,
    province      string,
    city          string,
    sess_per_user bigint,
    acts_per_user bigint,
    acts_per_sess bigint
) partitioned by (count_date bigint) stored as parquet