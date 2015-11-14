CREATE DATABASE IF NOT EXISTS chicago;
USE chicago;

CREATE TABLE IF NOT EXISTS data(
	zipcode INT,
	--Hive doesn't like ISO formatted dates...that's cool...
	time STRING COMMENT "UTC time in ISO format",
	temperature FLOAT COMMENT "Fahrenheit",
	dew_point FLOAT COMMENT "Fahrenheit",
	humidity INT,
	sea_level FLOAT COMMENT "Inches of Mercury",
	visibility FLOAT COMMENT "Miles",
	wind_direction STRING COMMENT "Cardinal Direction",
	wind_speed FLOAT COMMENT "Miles per Hour",
	gust_speed FLOAT COMMENT "Miles per Hour",
	precipitation FLOAT COMMENT "Inches",
	events STRING COMMENT "Free Text",
	conditions STRING COMMENT "Free Text",
	wind_direction_deg INT COMMENT "Degrees"
)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
