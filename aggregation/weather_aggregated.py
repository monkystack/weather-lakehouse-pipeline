"""
Gold Layer: Aggregate daily weather data
"""
# Note:
# In Databricks, configuration is imported using:
# %run ../includes/configuration

from pyspark.sql.functions import (
    to_date, avg, max, min, sum,
    split, collect_list, flatten,
    array_distinct, concat_ws
)

# Read the processed Silver weather table
weather_processed = spark.read.table(f"{processed_folder_path}.weather_silver")

# Aggregate daily weather data
weather_aggregated = (
    weather_processed
    .withColumn("date", to_date("event_time"))
    .groupBy("date")
    .agg(
        avg("temperature").alias("avg_temp"),
        max("temp_max").alias("max_temp"),
        min("temp_min").alias("min_temp"),
        avg("humidity").alias("avg_humidity"),
        sum("hourly_rainfall").alias("total_rainfall"),
        sum("hourly_snowfall").alias("total_snowfall"),
        
        concat_ws(
            ", ",
            array_distinct(
                flatten(
                    collect_list(
                        split("weather_main", ",")
                    )
                )
            )
        ).alias("weather_conditions")
    )
)

# Write aggregated data to Gold table
weather_aggregated.write.mode('overwrite').format("delta").saveAsTable(f'{presentation_folder_path}.weather_gold')