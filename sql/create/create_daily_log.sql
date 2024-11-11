CREATE EXTERNAL TABLE IF NOT EXISTS user_log(
  event_time string COMMENT '이벤트발생시간',
  event_type string COMMENT '이벤트종류',
  product_id bigint COMMENT '제품ID',
  category_id bigint COMMENT '제품카테고리ID',
  category_code string COMMENT '카테고리코드',
  brand string COMMENT '브랜드이름',
  price decimal COMMENT '가격',
  user_id string COMMENT '사용자ID',
  user_session string COMMENT '사용자세션ID',
  part_dt string COMMENT '사용자세션ID'
  ) COMMENT '사용자 로그' PARTITIONED BY (
  part_year string,
  part_month string,
  part_day string,
  part_hour string
) STORED AS PARQUET  LOCATION   'hdfs://localhost:9000/user/hive/warehouse/user_log'
 TBLPROPERTIES (
    'parquet.column.index.access'='true',
    'parquet.compression'='SNAPPY'
 );