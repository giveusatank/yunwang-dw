create table if not exists dim.dim_sale_channel
(
    id     bigint,
    app_id string,
    code   string,
    name   string
)
    stored as textfile;