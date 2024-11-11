package org.example;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import org.slf4j.LoggerFactory;

import java.util.logging.Logger;

import static org.apache.spark.sql.functions.*;

public class Main {
    static String input_file_path = "/Users/jy_kim/Downloads/2019-Nov.csv";
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
            * */

            /* 1. 테이블 생성 */
            FileConfiguration fileConfiguration = new FileConfiguration();
            String create_table_sql = fileConfiguration.FileReader(create_file_path);
            sparkSession.sql(create_table_sql);

            Dataset<Row> dataset = sparkSession.read().option("header", "true").csv(input_file_path)
                .select(
                    functions.col("event_time"),
                    functions.col("event_type"),
                    functions.col("product_id").cast("long"),
                    functions.col("category_id").cast("long"),
                    functions.col("category_code"),
                    functions.col("brand"),
                    functions.col("price").cast("decimal"),
                    functions.col("user_id"),
                    functions.col("user_session")
                )
                .withColumn("part_dt", date_format(col("event_time"), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("part_year", year(col("event_time")).cast("string"))
                .withColumn("part_month", month(col("event_time")).cast("string"))
                .withColumn("part_day", dayofmonth(col("event_time")).cast("string"))
                .withColumn("part_hour", hour(col("event_time")).cast("string")) ;
            // dataset.show(100, false);

            /* 2. daily partition 처리, part_dt(KST 기준)  */
            dataset.write()
                .partitionBy("part_year","part_month","part_day","part_hour")
                .mode("append")
                .option("compression", "snappy")
                .parquet(hive_wearhouse_path + "user_log");


            /* 3. delta backup  */
            dataset.write().format("delta").save(hdfs_path + "delta_log");
        } catch (Exception e) {
            e.printStackTrace();

            /* 장애 복구 - delta
            * 마지막것 전체 overwrite ??
            *   */

            /* delta test */
//            DeltaTable deltaTable = DeltaTable.forPath(sparkSession, delta_path);
//            deltaTable.delete(functions.col("event_type").equalTo("cart"));

            Dataset<Row> delta_ds = sparkSession.read().format("delta")
                    .load(delta_path);

            delta_ds.show(10, false) ;
            delta_ds.write().partitionBy("part_year","part_month","part_day","part_hour")
                    .mode("overwrite")
                    .option("compression", "snappy")
                    .parquet(hive_wearhouse_path + "user_log");
//            Dataset<Row> dt1 = sparkSession.read().parquet("hdfs://localhost:9000/user/hive/warehouse/user_log");
//            dt1.show(100, false);

        } finally {

        }

    }
}