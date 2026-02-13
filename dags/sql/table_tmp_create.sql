CREATE 
OR REPLACE TABLE `{{ BQ_DATASET }}.{{ base_file_path(params) }}` AS 
SELECT 
  MD5(
    CONCAT(
      COALESCE(
        CAST(VendorID AS STRING), 
        ""
      ), 
      COALESCE(
        CAST(tpep_pickup_datetime AS STRING), 
        ""
      ), 
      COALESCE(
        CAST(tpep_dropoff_datetime AS STRING), 
        ""
      ), 
      COALESCE(
        CAST(PULocationID AS STRING), 
        ""
      ), 
      COALESCE(
        CAST(DOLocationID AS STRING), 
        ""
      )
    )
  ) AS unique_row_id, 
  "{{ base_file_path(params) }}" AS filename, 
  * 
FROM 
  `{{ BQ_DATASET }}.{{ base_file_path(params) }}_ext`;
