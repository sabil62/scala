import com.typesafe.scalalogging.LazyLogging
import functions.date.formatDate
import functions.get_df_from_tablename.{
  get_df_from_tableName,
  update_column_of_table
}
import functions.update_parquet.update_parquet
import org.apache.spark.sql.functions.{col, current_timestamp, when}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.time.format.DateTimeFormatter

object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("mysql config")
      .master("local[*]")
      .getOrCreate();

    //################# LOGGING TEST
    logger.info("Application started!")
    logger.debug("This is a debug message")
    logger.error("An error occurred")
    //#################

    val df_config = get_df_from_tableName(spark = spark, tableName = "config");
    df_config.show()

    try {
      val df_config_arr_orginal = df_config.collect();
      var row_index = 1;
      df_config_arr_orginal.foreach(row => {
        val df_config_table_updated =
          get_df_from_tableName(spark = spark, tableName = "config");

        val source_table = row.getAs[String]("source_table");
        val hadoop_destination = row.getAs[String]("hadoop_destination");
        val is_increment = row.getAs[Int]("is_increment");

        logger.info(
          s"########## $source_table ----> $hadoop_destination -----> $is_increment  ##########"
        );
        val df_source =
          get_df_from_tableName(spark = spark, tableName = source_table);

        // ######################################  UPDATE CONFIG TABLE (START DATE) #############################
        val df_updated_config_startDate = df_config_table_updated.withColumn(
          "start_date",
          when(col("id") === row_index, current_timestamp()).otherwise(
            col("start_date")
          )
        );
        df_updated_config_startDate.show();
        update_column_of_table(
          df_source = df_updated_config_startDate,
          tableName = "config"
        )
        //####################################################################################################

        if (is_increment == 0) {
          //full load

          df_source.write
            .format("parquet")
            .mode(SaveMode.Append)
            .save(hadoop_destination);
        } else if (is_increment == 1) {
          //delta load
          df_source.show(20)
          println("############## DELTA LOAD LOADING ################")

          //first calculate latest date from filter column
          val filter_column = row.getAs[String]("filter_column");
          println(s"filter column is ${filter_column}")
          val row_latest_date = df_source
            .select(col(filter_column))
            .sort(col(filter_column).desc)
            .first();
          println(
            s"ROW LATEST DATA select is ${row_latest_date.getAs[String](filter_column)}"
          )

          val customFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

          val latest_date = formatDate(
            dateVariable = row_latest_date.getAs[String](filter_column),
            formatter = customFormatter
          );
          val previous_date = latest_date.minusDays(1);

          logger.info(
            s"################# LATEST DATE ${latest_date} ----> PREVIOUS DATE ${previous_date} #######################"
          )

//          ########### UPDATE/MERGE the filtered delta data to parquet file
          update_parquet(
            spark = spark,
            df_current = df_source,
            filter_column = filter_column,
            filter_value = previous_date,
            destination_path = hadoop_destination
          );

          println(" DELTA LOAD FINISHED")

        }

        // ############################ UPDATE CONFIG TABLE (END DATE)#############################
        val df_updated_config_endDate =
          df_updated_config_startDate.withColumn(
            "end_date",
            when(col("id") === row_index, current_timestamp()).otherwise(
              col("end_date")
            )
          );
        df_updated_config_endDate.show();
        update_column_of_table(
          df_source = df_updated_config_endDate,
          tableName = "config"
        )
        //#######################################################################################
        row_index += 1

      })

      println(" SUCCESSFULLY CONDUCTED ALL STEPS")

    } catch {
      case e: Exception =>
        logger.info(
          s"ERROR IS >>>>>>>>>>>>>>>>>>>>> ${e.getMessage} <<<<<<<<<<<<<<<<<"
        )
    } finally {
      spark.stop()
    }
  }

}
