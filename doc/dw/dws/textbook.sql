#dws层的建表语句

#这张表是会话级别的 用户在一次会话中的对教材的使用情况
create table if not exists dws_textbook_used_session
(
    product_id         string,
    company            string,
    country            string,
    province           string,
    city               string,
    location           string,
    user_id            string,
    device_id          string,
    group_id           string,
    passive_obj        string,
    sum_time_consume   bigint,
    avg_time_consume   bigint,
    start_action       string,
    start_action_count bigint,
    action_count       bigint
) partitioned by (count_date bigint)
stored as parquet

#按天分区 统计的是每天教材的pv,uv,累计阅读时长等
create table if not exists dws_textbook_used_daily
(
    product_id         string,
    company            string,
    country            string,
    province           string,
    city               string,
    location           string,
    passive_obj        string,
    sum_time_consume   bigint,
    avg_time_consume   bigint,
    start_action       string,
    start_action_count bigint,
    action_count       bigint,
    user_count         bigint
) partitioned by (count_date bigint) stored as parquet

#用户对于教材的历史使用情况
create table if not exists dws_textbook_used_total
(
    product_id         string,
    company            string,
    country            string,
    province           string,
    city               string,
    location           string,
    user_id            string,
    passive_obj        string,
    sum_time_consume   bigint,
    avg_time_consume   bigint,
    start_action       string,
    start_action_count bigint,
    action_count       bigint,
    count_date         bigint
) stored as parquet

#将dws_textbook_used_total按照学科相关维度展开形成宽表
create table if not exists dws_textbook_used_total_wide
(
    product_id         string,
    company            string,
    country            string,
    province           string,
    city               string,
    location           string,
    passive_obj        string,
    zxxkc              string,
    nj                 string,
    fascicule_name     string,
    rkxd               string,
    year               string,
    publisher          string,
    user_id            string,
    sum_time_consume   bigint,
    avg_time_consume   bigint,
    action_count       bigint,
    start_action_count bigint,
    start_action       string,
    count_date         bigint
) stored as parquet


