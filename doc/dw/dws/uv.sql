create table dws_uv_daily
(
    product_id        string,
    company           string,
    remote_addr       string,
    country           string,
    province          string,
    city              string,
    location          string,
    active_user       string,
    device_id         string,
    first_access_time string,
    last_access_time  string,
    action_count      bigint
)
    partitioned by (count_date int)
    STORED AS parquet;

create table dws_uv_total
(
    product_id        string,
    company           string,
    remote_addr       string,
    country           string,
    province          string,
    city              string,
    location          string,
    active_user       string,
    device_id         string,
    first_access_time string,
    last_access_time  string,
    action_count      bigint,
    count_date        int
)
    STORED AS parquet;

create table dws_uv_increase
(
    product_id        string,
    company           string,
    remote_addr       string,
    country           string,
    province          string,
    city              string,
    location          string,
    active_user       string,
    device_id         string,
    first_access_time string,
    last_access_time  string,
    action_count      bigint
)
    partitioned by (count_date int)
    STORED AS parquet;


