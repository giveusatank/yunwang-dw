CREATE TABLE `dim_textbook`
(
    `id`             bigint(20)   NOT NULL COMMENT '基于编码id',
    `name`           varchar(128) DEFAULT NULL COMMENT '教材名称',
    `sub_heading`    varchar(32)  DEFAULT NULL COMMENT '教材副标题',
    `rkxd`           mediumint(9) DEFAULT NULL COMMENT '学段（任课学段）',
    `rkxd_name`      varchar(64)  DEFAULT NULL COMMENT '任课学段名称',
    `nj`             mediumint(9) NOT NULL COMMENT '年级',
    `nj_name`        varchar(64)  DEFAULT NULL COMMENT '年级名称',
    `zxxkc`          mediumint(9) DEFAULT NULL COMMENT '学科（中小学课程）',
    `zxxkc_name`     varchar(64)  DEFAULT NULL COMMENT '学科名称',
    `fascicule`      varchar(9)   DEFAULT NULL COMMENT '册别',
    `fascicule_name` varchar(64)  DEFAULT NULL COMMENT '册别名称',
    `year`           mediumint(9) DEFAULT NULL COMMENT '出版年份',
    `isbn`           varchar(32)  NOT NULL COMMENT 'isbn号',
    `publisher`      varchar(32)  DEFAULT NULL COMMENT '出版社',
    `res_size`       bigint(20)   DEFAULT '0' COMMENT '教材资源大小',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='教材维度';



INSERT
dim_textbook
select id,
       name,
       sub_heading,
       rkxd,
       rkxd_name,
       nj,
       nj_name,
       zxxkc,
       zxxkc_name,
       fascicule,
       fascicule_name,
       `year`,
       publisher,
       isbn,
       res_size
from p_textbook
where s_state = '110';


CREATE TABLE `dim_textbook`
(
    `id`             string,
    `name`           string,
    `sub_heading`    string,
    `rkxd`           int,
    `rkxd_name`      string,
    `nj`             int,
    `nj_name`        string,
    `zxxkc`          int,
    `zxxkc_name`     string,
    `fascicule`      string,
    `fascicule_name` string,
    `year`           int,
    `isbn`           string,
    `publisher`      string,
    `res_size`       int
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '~'
STORED AS TEXTFILE;

sqoop import -D mapred.job.queue.name=dwq --connect jdbc:mysql://172.30.0.9:3306/bi --username root --password rjszgs2019 --table dim_textbook  --fields-terminated-by '~' --lines-terminated-by "\n" --hive-import --hive-overwrite --create-hive-table --hive-table dim.dim_textbook --delete-target-dir