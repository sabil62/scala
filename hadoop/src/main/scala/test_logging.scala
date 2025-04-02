import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{SaveMode, SparkSession}
import functions.get_df_from_tablename.{
  get_df_from_tableName,
  update_column_of_table
}
import org.apache.spark.sql.functions.{col, current_timestamp, when}

object test_logging extends LazyLogging {
  // Ensure log directory exists
  def main(args: Array[String]): Unit = {

    logger.debug("This is Lazy Logging ;-)")

    val spark = SparkSession
      .builder()
      .appName("mysql config")
      .master("local[*]")
      .getOrCreate();

    val df_config = get_df_from_tableName(spark = spark, tableName = "config");
    df_config.show()

    // ############################ UPDATE CONFIG TABLE #############################
    val df_config_table_updated =
      get_df_from_tableName(spark = spark, tableName = "config");
    val df_updated_config_startDate = df_config_table_updated.withColumn(
      "start_date",
      when(col("id") === 1, current_timestamp()).otherwise(
        col("start_date")
      )
    );
    df_config.printSchema();
    df_updated_config_startDate.printSchema();

    df_updated_config_startDate.show();
    update_column_of_table(
      df_source = df_updated_config_startDate,
      tableName = "config"
    )

    val df_new = df_updated_config_startDate.withColumn(
      "end_date",
      when(col("id") === 1, current_timestamp()).otherwise(col("end_date"))
    )
    update_column_of_table(
      df_source = df_new,
      tableName = "config"
    )
    //############################################################################

//    try {
//      val df_config_arr_orginal = df_config.collect();
//      var row_index = 1;
//      df_config_arr_orginal.foreach(row => {
//        val df_config_table_updated =
//          get_df_from_tableName(spark = spark, tableName = "config");
//
//        val source_table = row.getAs[String]("source_table");
//        val hadoop_destination = row.getAs[String]("hadoop_destination");
//        val is_increment = row.getAs[Int]("is_increment");
//
//        logger.info(
//          s"########## $source_table ----> $hadoop_destination -----> $is_increment  ##########"
//        );
//        val df_source =
//          get_df_from_tableName(spark = spark, tableName = source_table);
//
//        // ############################ UPDATE CONFIG TABLE #############################
//        val df_updated_config_startDate = df_config_table_updated.withColumn(
//          "start_date",
//          when(col("id") === row_index, current_timestamp()).otherwise(
//            col("start_date")
//          )
//        );
//        df_updated_config_startDate.show();
//        update_column_of_table(
//          df_source = df_updated_config_startDate,
//          tableName = "config"
//        )
//        //############################################################################
//
//        if (is_increment == 0) {
//          //full load
//        } else if (is_increment == 1) {
//          println(" DELTA LOAD FINISHED")
//        }
//
//        // ############################ UPDATE CONFIG TABLE #############################
//        val df_updated_config_endDate =
//          df_updated_config_startDate.withColumn(
//            "end_date",
//            when(col("id") === row_index, current_timestamp()).otherwise(
//              col("end_date")
//            )
//          );
//        df_updated_config_endDate.show();
//        update_column_of_table(
//          df_source = df_updated_config_endDate,
//          tableName = "config"
//        )
//        //############################################################################
//        row_index += 1
//
//      })
//
//      println(" SUCCESSFULLY CONDUCTED ALL STEPS")
//
//    } catch {
//      case e: Exception =>
//        logger.info(
//          s"ERROR IS >>>>>>>>>>>>>>>>>>>>> ${e.getMessage} <<<<<<<<<<<<<<<<<"
//        )
//    } finally {
//      spark.stop()
//    }
  }

}
