create table ads_uv_daily
(
    product_id       string,
    company          string,
    country          string,
    province         string,
    city             string,
    location         string,
    user_count       bigint,
    action_count     bigint,
    session_count    bigint,
    mark_date        string
)
    partitioned by (count_date string)
    stored as textfile;

create table ads_uv_total
(
    product_id       string,
    company          string,
    country          string,
    province         string,
    city             string,
    location         string,
    user_count       bigint,
    action_count     bigint,
    session_count    bigint,
    mark_date        string
)
    partitioned by (count_date string)
    stored as textfile;


create table ads_uv_increase
(
    product_id       string,
    company          string,
    country          string,
    province         string,
    city             string,
    location         string,
    user_count       bigint,
    action_count     bigint,
    session_count    bigint,
    mark_date        string
)
    partitioned by (count_date string)
    stored as textfile;