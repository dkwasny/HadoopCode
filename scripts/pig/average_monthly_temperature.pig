/* Generates the average temperature for every month of the year
 * and calculates how far a particular temperature reading
 * is from the average temperature for the month.
 * 
 * the average.
 *
 * Input data is generated from net.kwaz.chicago.pig.GeneratePigInput 
 *
 * Parameters:
 *   input_path
 *   output_path
 *
 * Output Schema:
 *   zipcode,
 *   time_utc,
 *   temperature,
 *   average_temperature_for_month,
 *   distance_from_average
 */

-- Load the data
raw_data = LOAD '$input_path'
	USING PigStorage(',')
	AS (
		zipcode:int,
		time_utc:datetime,
		temperature_f:float,
		dew_point_f:float,
		humidity:int,
		sea_level_inches_of_mercury:float,
		visibility_miles:float,
		wind_direction:chararray,
		wind_speed_mph:float,
		gust_speed_mph:float,
		precipitation_inches:float,
		events:chararray,
		conditions:chararray,
		wind_direction_degrees:int
	);

-- Filter out any bad data (Add more rules as needed)
filtered_data = FILTER raw_data BY
	temperature_f < 200.0f
	AND temperature_f > -100.0f;

-- Group all data by month of year
grouped_by_month = GROUP filtered_data BY GetMonth(time_utc);

-- Get the average temperature by month
average_temp_by_month = FOREACH grouped_by_month GENERATE
	group AS month,
	AVG(filtered_data.temperature_f) AS avg_temp_for_month;

-- Join the average temp data with raw data
joined_data = JOIN
	filtered_data BY GetMonth(time_utc),
	average_temp_by_month BY month;

-- Generate the final output
output_data = FOREACH joined_data GENERATE
	zipcode,
	time_utc,
	temperature_f,
	avg_temp_for_month,
	(temperature_f - avg_temp_for_month) AS distance_from_avg;

-- Sort the output by distance_from_avg
sorted_output = ORDER output_data BY distance_from_avg DESC;

-- Write the output to HDFS
STORE sorted_output INTO '$output_path';
