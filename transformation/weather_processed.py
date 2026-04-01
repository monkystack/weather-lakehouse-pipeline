"""
Silver Layer: Process raw weather data and upsert to Silver table
"""

# Note:
# In Databricks, configuration is imported using:
# %run ../includes/configuration

from pyspark.sql.functions import col, lit, coalesce, from_unixtime, concat_ws, expr

# Read the path of the latest raw weather file
file_path = dbutils.fs.head(f"{raw_folder_path}/latest_weather.txt")
weather_df = spark.read.option('header', True).json(file_path)

# Handle rain column safely
if "rain" in weather_df.columns:
    rain_col = coalesce(col("rain.`1h`"), lit(0.00))
else:
    rain_col = lit(0.00)

# Handle snow column safely
if "snow" in weather_df.columns:
    snow_col = coalesce(col("snow.`1h`"), lit(0.00))
else:
    snow_col = lit(0.00)

# Select and rename relevant columns, including flattening weather array
selected_weather_df = weather_df.select(
    col("name").alias("city"),
    col("sys.country").alias("country"),
    col("coord.lat").alias("latitude"),
    col("coord.lon").alias("longitude"),
    from_unixtime(col("dt")).cast("timestamp").alias("event_time"),
    col("main.temp").alias("temperature"),
    col("main.feels_like"),
    col("main.humidity"),
    col("main.pressure"),
    col("main.temp_max"),
    col("main.temp_min"),
    col("wind.speed").alias("wind_speed"),
    col("wind.deg").alias("wind_degree"),
    col("clouds.all").alias("cloud_coverage"),
    col("visibility"),
    rain_col.alias("hourly_rainfall"),
    snow_col.alias("hourly_snowfall"),
    concat_ws(",", expr("transform(weather, x -> x.main)")).alias("weather_main"),
    concat_ws(",", expr("transform(weather, x -> x.description)")).alias("weather_description"),
    from_unixtime(col("sys.sunrise")).cast("timestamp").alias("sunrise"),
    from_unixtime(col("sys.sunset")).cast("timestamp").alias("sunset")
)

# Reorder columns: put event_time first
cols = ["event_time"] + [c for c in selected_weather_df.columns if c != "event_time"]
df_reordered = selected_weather_df.select(cols)

# Upsert processed data to Silver table
upsert_to_silver(df_reordered, 'open_weather_project.processed.weather_silver','t.event_time = s.event_time')

# Read Silver table for further processing (no display/print)
weather_processed = spark.read.table(f"{processed_folder_path}.weather_silver")