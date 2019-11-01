#ads层建表语句

#历史累计教材的统计
create table if not exists ads_textbook_used_total
(
    product_id            string,
    company               string,
    sum_time_consume      bigint,
    avg_time_consume      bigint,
    simulate_time_consume bigint,
    start_action_count    bigint,
    action_count          bigint,
    user_count            bigint,
    mark_date             string
) partitioned by (count_date bigint)
stored as textfile

#历史累计学科下 所有教材的统计
create table if not exists ads_textbook_zxxkc_used_total
(
    product_id            string,
    company               string,
    zxxkc                 string,
    sum_time_consume      bigint,
    avg_time_consume      bigint,
    simulate_time_consume bigint,
    start_action_count    bigint,
    action_count          bigint,
    user_count            bigint,
    mark_date             string
) partitioned by (count_date bigint)
stored as textfile

#历史累计学科、年级下 所有教材的统计
create table if not exists ads_textbook_nj_used_total
(
    product_id            string,
    company               string,
    zxxkc                 string,
    nj                    string,
    sum_time_consume      bigint,
    avg_time_consume      bigint,
    simulate_time_consume bigint,
    start_action_count    bigint,
    action_count          bigint,
    user_count            bigint,
    mark_date             string
) partitioned by (count_date bigint)
stored as textfile

#历史累计学科下 所有不同的教材的统计排名情况
create table if not exists ads_textbook_per_used_total
(
    product_id            string,
    company               string,
    passive_obj           string,
    rkxd                  string,
    zxxkc                 string,
    nj                    string,
    fascicule             string,
    sum_time_consume      bigint,
    avg_time_consume      bigint,
    simulate_time_consume bigint,
    start_action_count    bigint,
    action_count          bigint,
    user_count            bigint,
    mark_date             string
) partitioned by (count_date bigint)
stored as textfile