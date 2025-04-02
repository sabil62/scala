package functions

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object update_parquet extends LazyLogging {
  def update_parquet(
      spark: SparkSession,
      df_current: DataFrame,
      filter_column: String,
      filter_value: Any,
      destination_path: String
  ) = {
    // Try to read the Parquet file and check if it's empty
    val df_existing =
      try {
        val tempDf = spark.read.format("parquet").load(destination_path)
        if (tempDf.isEmpty) {
          logger.info("Parquet file exists but is empty")
          None
        } else {
          Some(tempDf)
        }
      } catch {
        case _: Exception =>
          logger.info("No valid Parquet file found")
          None
      }

    // Get current source data

    // Process based on existing data
    df_existing match {
      case Some(existingData) => {
        // Merge existing data with new data
        val df_combined = existingData.union(
          df_current.filter(col(filter_column) < filter_value)
        )

        val distinct_data = df_combined.dropDuplicates()

        distinct_data.write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .save(destination_path)

        logger.info(s"Delta load completed for $filter_column")
      }

      case None => {
        // No existing data, overwrite current data
        df_current.write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .save(destination_path)

        logger.info(s"Initial load completed")
      }
    }
  }

}
