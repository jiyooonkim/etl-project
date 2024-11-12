package org.example;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.logging.Logger;

import static org.apache.spark.sql.functions.*;

public class Main {
    static String input_file_path = "/Users/jy_kim/Downloads/2019-Dec.csv";
    static String hive_wearhouse_path = "hdfs://localhost:9000/user/hive/warehouse/";
    static String hdfs_path = "hdfs://localhost:9000/user/";
    static String hive_url = "jdbc:hive2://localhost:10000/default";
    static String create_file_path = "/Users/jy_kim/Downloads/etl-project/sql/create/create_daily_log.sql";
    static String delta_path = "hdfs://localhost:9000/user/delta_log/";

    public static void main(String[] args) {
        SparkSession.Builder builder = SparkSession.builder();
        builder.master("local[8]");
        builder.config("spark.driver.memory", "16g");
        builder.config("spark.driver.cores", 8);
        builder.config("spark.executor.memory", "16g");
        builder.config("spark.driver.bindAddress", "127.0.0.1");
        builder.config("spark.network.timeout", 100000);
        builder.config("spark.sql.session.timeZone", "Asia/Seoul");
        builder.config("spark.sql.hive.convertMetastoreOrc", "false");
        builder.config("spark.sql.parquet.writeLegacyFormat", "true");
        builder.config("spark.sql.warehouse.dir", hive_wearhouse_path);
        builder.config("spark.sql.hive.hiveserver2.jdbc.url", hive_url);
        builder.config("spark.hadoop.hive.exec.dynamic.partition", "true");
        builder.config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict");
        builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension");
        builder.config("hive.mapred.supports.subdirectories", "TRUE");  // 파티션 옵션 활성화
        builder.config("mapred.input.dir.recursive", "TRUE");           //
        builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
        builder.enableHiveSupport();
        builder.appName("Etl_Job");
        SparkSession sparkSession = builder.getOrCreate();


        try {
            /*
            * 1. 테이블생성 :  .sql 파일 읽기, CREATE EXTERNAL TABLE IF NOT EXISTS....
            * 2. hivewearhouse write(snappy.parquet)
            * 3. delta 에도 동일하게 write
            * 4. 배치 작업에 장애 발생시 delta로 백업
            * */

            /* 1. 테이블 생성 */
            FileConfiguration fileConfiguration = new FileConfiguration();
            String create_table_sql = fileConfiguration.FileReader(create_file_path);
            sparkSession.sql(create_table_sql);

            Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(input_file_path)
                .select(
                    col("event_time"),
                    col("event_type"),
                    col("product_id").cast("long"),
                    col("category_id").cast("long"),
                    col("category_code"),
                    col("brand"),
                    col("price").cast("decimal"),
                    col("user_id"),
                    col("user_session")
                )
                .withColumn("part_dt", date_format(col("event_time"), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("part_year", year(col("event_time")).cast("string"))
                .withColumn("part_month", month(col("event_time")).cast("string"))
                .withColumn("part_day", dayofmonth(col("event_time")).cast("string"))
                .withColumn("part_hour", hour(col("event_time")).cast("string"))
                .sample(false,0.2)
                .alias("dataset");
             dataset.show(100, false);

            /* 2. daily partition 처리, part_dt(KST 기준)  */
            try {
                dataset.write()
                    .partitionBy("part_year","part_month","part_day","part_hour")
                    .mode("append")
                    .option("compression", "snappy")
                    .parquet(hive_wearhouse_path + "user_log");

            } catch (Exception e){
                dataset.write()
                    .partitionBy("part_year","part_month","part_day","part_hour")
                    .mode("overwrite")
                    .option("compression", "snappy")
                    .parquet(hive_wearhouse_path + "user_log");
            }


             /* 3. delta backup - update & insert */
            try {


//             DeltaTable deltaTable1 = DeltaTable.forPath(sparkSession, delta_path);
//            deltaTable1.toDF().show(30, false);
//                deltaTable1.as("old_dt")
//                    .merge(
//                        dataset.as("new_dt"),
//                        "new_dt.user_id = old_dt.user_id"
//                    )
//                    .whenMatched("new_dt.event_time = old_dt.event_time and  old_dt.event_type = new_dt.event_type and old_dt.product_id=new_dt.product_id").updateAll().whenNotMatched().insertAll().execute();

            DeltaTable deltaTable = DeltaTable.forPath(sparkSession, delta_path);
            deltaTable.as("old_dt")
                    .merge(
                        dataset.as("new_dt"),
                        "new_dt.event_time = old_dt.event_time and new_dt.user_id = old_dt.user_id and old_dt.event_type = new_dt.event_type"
                    )
                    .whenNotMatched().insertAll().execute();
            } catch (Exception e){
                dataset.write().mode("overwrite")
                    .format("delta")
                    .save(hdfs_path + "delta_log");
            }
        } catch (Exception e) {
            e.printStackTrace();
            /* 4. 델타 백업 */
            Dataset<Row> delta_ds = sparkSession.read().format("delta").load(delta_path);
            delta_ds.write().partitionBy("part_year","part_month","part_day","part_hour")
                    .mode("overwrite")
                    .option("compression", "snappy")
                    .parquet(hive_wearhouse_path + "user_log");
        }

    }
}