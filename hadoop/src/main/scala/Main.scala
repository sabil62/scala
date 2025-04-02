import com.typesafe.scalalogging.LazyLogging
import functions.date.formatDate
import functions.df_functions.{get_df_from_tableName, run_sql}
import functions.update_parquet.update_parquet
import org.apache.spark.sql.functions.{col, current_timestamp}
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
    //#################

    //################# ARGUMENTS #################
    val argument1 = args.lift(0).getOrElse("default_argument_file");
    val argument2 = args.lift(1).getOrElse("default_argument_2");

    logger.info(argument1, "_________________", argument2);

    if (argument1 == "hdfs") {
      logger.info("LOGGING TEST - LOAD MYSQL DATA TO HDFS")
    }
    //################# ############# #################

    val df_config = get_df_from_tableName(spark = spark, tableName = "config");
    df_config.show()

    try {
      val df_config_arr_orginal = df_config.collect();
      var row_index = 1;
      df_config_arr_orginal.foreach(row => {
        val source_table = row.getAs[String]("source_table");
        val hadoop_destination = row.getAs[String]("hadoop_destination");
        val is_increment = row.getAs[Int]("is_increment");

        logger.info(
          s"########## $source_table ----> $hadoop_destination -----> $is_increment  ##########"
        );
        val df_source =
          get_df_from_tableName(spark = spark, tableName = source_table);

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
//        val currentTime = LocalDateTime.now();
        run_sql(sqlStatement =
          s"UPDATE config SET start_date=${current_timestamp()} ,end_date=${current_timestamp()} where id=$row_index"
        )

        //#######################################################################################
        row_index += 1

      })

      logger.info(
        " }}}}}}}}}}}}}}}}}} SUCCESSFULLY CONDUCTED ALL STEPS {{{{{{{{{{{{{{{{{"
      )

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
