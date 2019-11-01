create external table if not exists ods.ods_zyk_pep_cn_resource(
rid			   string,
file_md5       string,
r_name         string,
r_desc         string,
r_key          string,
r_people       string,
r_language     string,
r_thumb        string,
r_ext          string,
cid1           string,
cid2           string,
cid3           string,
rtype1         string,
rtype2         string,
tid1_path      string,
tid2_path      string,
c_version      string,
c_type         string,
c_date_start   string,
c_date_end     string,
c_owner        string,
c_scope        string,
c_scope_has    string,
c_author       string,
c_from         string,
create_time    string,
publish_time   string,
r_status       string,
uid            string,
uname          string,
downs          string,
favs           string,
hits           string,
stars          string,
attachs        string,
r_grade        string,
r_birth_year   string,
r_senv         string,
r_sver_min     string,
r_sver_max     string,
r_sdesc        string,
r_splat        string,
c_sug          string,
c_authorship   string,
c_payfor       string,
c_limit_area   string,
c_limit_user   string,
c_limit_plat   string,
r_version      string,
rpy            string,
tid3_path      string,
tid4_path      string,
tid5_path      string,
row_timestamp  string,
row_status     string
)
partitioned by (put_date string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile location '/pep_cloud/business/ods/ods_zyk_pep_cn_resource';



create external table if not exists ods.ods_zyk_pep_cn_file(
file_md5                  string,
file_url                  string,
file_oname                string,
file_name                 string,
file_extension            string,
file_size                 string,
file_url_view             string,
file_url_view_deal        string,
file_transcoded           string,
row_timestamp             string,
row_status                string
)
partitioned by (put_date string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile location '/pep_cloud/business/ods/ods_zyk_pep_cn_file';




create external table if not exists ods.ods_zyk_pep_cn_attach(
attach_id                 string,
rid                       string,
file_md5                  string,
row_timestamp             string,
row_status                string
)
partitioned by (put_date string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile location '/pep_cloud/business/ods/ods_zyk_pep_cn_attach';


create external table if not exists ods.ods_zyk_pep_cn_tree(
tid            string,
tname          string,
tcode          string,
parent_id      string,
tree_type      string,
cid            string,
corder         string,
chapter_id     string,
row_timestamp             string,
row_status                string
)
partitioned by (put_date string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile location '/pep_cloud/business/ods/ods_zyk_pep_cn_tree';

create external table if not exists ods.ods_zyk_pep_cn_resource_push(
pid            string,
sysid          string,
tid            string,
chapter_id     string,
rid            string,
attach_id      string,
r_flg          string,
r_page         string,
pos_description string,
ex_turnpage    string,
r_zynrlx       string,
r_dzwjlx       string,
r_jxsx         string,
ex_gallery     string,
r_year         string,
source_batch   string,
file_md5       string,
file_url       string,
file_extension string,
file_oname     string,
status_public  string,
status_msg     string,
is_first       string,
createtime     string,
updatetime     string,
creator        string,
uid            string,
row_timestamp             string,
row_status                string
)
partitioned by (put_date string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile location '/pep_cloud/business/ods/ods_zyk_pep_cn_resource_push';


