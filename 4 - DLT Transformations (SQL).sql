-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE joined_streams
(CONSTRAINT null_columns EXPECT (RideID IS NOT NULL and Ride_end_time_stamp IS NOT NULL and Ride_length_estimate_mins IS NOT NULL and City IS NOT NULL and Ride_start_time_stamp is not NULL) ON VIOLATION DROP ROW)
PARTITIONED BY (City) 
COMMENT "ride start and ride end streams joined & partitioned by city"
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
AS
SELECT Ride_start_time_stamp, Ride_end_time_stamp, a.RideID, Ride_length_estimate_mins, City
  FROM STREAM(delta_stream1) AS a
  JOIN STREAM(delta_stream2) AS b
      ON a.RideID = b.RideID
      AND Ride_end_time_stamp <= Ride_start_time_stamp + INTERVAL 1 HOUR;



-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE unix_format_joined_streams
(CONSTRAINT timestamps_in_order EXPECT (ride_endtime_unix > ride_starttime_unix) ON VIOLATION DROP ROW)
COMMENT "convert to unix for calculation"
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
AS
SELECT Ride_end_time_stamp, UNIX_TIMESTAMP(Ride_end_time_stamp) as ride_endtime_unix, UNIX_TIMESTAMP(Ride_start_time_stamp) as ride_starttime_unix, Ride_length_estimate_mins, RideID, City 
  FROM STREAM(LIVE.joined_streams);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE joined_streams_ride_length
(CONSTRAINT valid_ride_length EXPECT (actual_ride_length > 0) ON VIOLATION DROP ROW)
COMMENT "subtract unix timestamps to get actual ride length"
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
AS
SELECT Ride_end_time_stamp, ride_endtime_unix, ride_starttime_unix, (ride_endtime_unix - ride_starttime_unix) AS actual_ride_length, Ride_length_estimate_mins, RideID, City 
  FROM STREAM(LIVE.unix_format_joined_streams);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE model_bias_calculation
(CONSTRAINT reasonable_difference EXPECT (difference_in_minutes between -60 and 60) ON VIOLATION DROP ROW)
COMMENT "calculates the difference between the ETA estimation and the actual ride length resulting in the model error/bias"
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
AS
SELECT *, (Ride_length_estimate_mins - actual_ride_length) AS difference_in_minutes
  FROM STREAM(LIVE.joined_streams_ride_length);

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE average_bias_per_city_per_window 
CONSTRAINT null_value EXPECT (City IS NOT NULL AND model_bias IS NOT NULL AND newest_timestamp IS NOT NULL)
COMMENT "sliding window calculating the model error/bias for the past 30 mins updating every minute and grouping by city"
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
AS
  SELECT
    City,
    AVG(difference_in_minutes) AS model_bias,
    window(Ride_end_time_stamp, '30 minute', '1 minute').
    end as newest_timestamp
  FROM
    STREAM(LIVE.model_bias_calculation) 
  WATERMARK Ride_end_time_stamp
  DELAY OF INTERVAL 0 SECONDS
  GROUP BY
    window(Ride_end_time_stamp, '30 minutes', '1 minute'),
    City;

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE model_bias_gold
COMMENT "the most recent model bias average for each city"
TBLPROPERTIES ("myCompanyPipeline.quality" = "gold")
AS SELECT City, max_by(model_bias, newest_timestamp) AS most_recent_model_bias
FROM STREAM(LIVE.average_bias_per_city_per_window)
GROUP BY city
