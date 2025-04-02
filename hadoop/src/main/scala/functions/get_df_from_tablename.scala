package functions

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.DriverManager
import java.time.LocalDateTime

object get_df_from_tablename extends LazyLogging {
  val jdbc_url = "jdbc:mysql://localhost:3306/transactions"
  val user = "root"
  val password = "admin"

  def get_df_from_tableName(
      spark: SparkSession,
      tableName: String,
      jdbcUrl: String = jdbc_url
  ) = {
    spark.read
      .format("jdbc")
      .options(
        Map(
          "url" -> jdbcUrl,
          "dbTable" -> tableName,
          "user" -> user,
          "password" -> password
        )
      )
      .load()
  }

  // this function is not need (deprecated)
  def update_column_of_table(
      df_source: DataFrame,
      tableName: String,
      jdbcUrl: String = jdbc_url
  ): Unit = {
    //check table is empty or not
    if (df_source.isEmpty) {
      logger.warn("SOURCE TABLE IS EMPTY")
    }

    // temporary table name for now
    val tempTable = s"temp_${tableName}_${System.currentTimeMillis()}"

    //create a temporary table (later be renamed)
    df_source.write
      .format("jdbc")
      .options(
        Map(
          "url" -> jdbcUrl,
          "dbTable" -> tempTable, //this is the main part to create a new table
          "user" -> user,
          "password" -> password,
          "driver" -> "com.mysql.cj.jdbc.Driver"
        )
      )
      .mode(SaveMode.Overwrite)
      .save();

    //now rename the temporary table to orginal tableName
    val conn = DriverManager.getConnection(jdbcUrl, user, password);
    try {
      val stmt = conn.createStatement();

      stmt.execute(s"DROP TABLE IF EXISTS ${tableName}");
      stmt.execute(s"RENAME TABLE ${tempTable} TO ${tableName}")
    } catch {
      case e => logger.info(s"ERROR IS >>>>>>>>>>>>>${e.getMessage}<<<<<<<<<<")
    } finally {
      conn.close()
    }

    logger.info(s"Successfully updated table $tableName")
  }

}
