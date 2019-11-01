create table if not exists dws_order_detail_total
(
    app_id          string,
    sale_channel_id string,
    user_count      string,
    country         string,
    province        string
) partitioned by (count_date string) stored as parquet