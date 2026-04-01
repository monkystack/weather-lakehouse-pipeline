"""
Configuration file for data pipeline paths (Databricks environment)

This file centralizes all storage paths to avoid hardcoding values
across multiple scripts and notebooks.
"""

# -----------------------------
# Bronze Layer (Raw Data)
# -----------------------------
raw_folder_path = '/Volumes/open_weather_project/raw/raw_volume'

# -----------------------------
# Silver Layer (Processed Data)
# -----------------------------
processed_folder_path = 'open_weather_project.processed'

# -----------------------------
# Gold Layer (Presentation Data)
# -----------------------------
presentation_folder_path = 'open_weather_project.presentation'