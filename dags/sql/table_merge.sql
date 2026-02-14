MERGE INTO `{{ GCP_PROJECT_ID }}.{{ BQ_DATASET }}.yellow_tripdata` T USING `{{ GCP_PROJECT_ID }}.{{ BQ_DATASET }}.{{ base_file_path(params, data_interval_start) }}` S ON T.unique_row_id = S.unique_row_id WHEN NOT MATCHED THEN INSERT (
  unique_row_id, filename, VendorID, 
  tpep_pickup_datetime, tpep_dropoff_datetime, 
  passenger_count, trip_distance, 
  RatecodeID, store_and_fwd_flag, 
  PULocationID, DOLocationID, payment_type, 
  fare_amount, extra, mta_tax, tip_amount, 
  tolls_amount, improvement_surcharge, 
  total_amount, congestion_surcharge
) 
VALUES 
  (
    S.unique_row_id, S.filename, S.VendorID, 
    S.tpep_pickup_datetime, S.tpep_dropoff_datetime, 
    S.passenger_count, S.trip_distance, 
    S.RatecodeID, S.store_and_fwd_flag, 
    S.PULocationID, S.DOLocationID, 
    S.payment_type, S.fare_amount, S.extra, 
    S.mta_tax, S.tip_amount, S.tolls_amount, 
    S.improvement_surcharge, S.total_amount, 
    S.congestion_surcharge
  );
