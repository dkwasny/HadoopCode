CREATE DATABASE IF NOT EXISTS chicago;
USE chicago;

CREATE TABLE IF NOT EXISTS data(
	id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
	zipcode INTEGER NOT NULL,
	time VARCHAR(30) COMMENT "UTC time in ISO format",
	temperature FLOAT COMMENT "Fahrenheit",
	dew_point FLOAT COMMENT "Fahrenheit",
	humidity SMALLINT,
	sea_level FLOAT COMMENT "Inches of Mercury",
	visibility FLOAT COMMENT "Miles",
	wind_direction VARCHAR(20) COMMENT "Cardinal Direction",
	wind_speed FLOAT COMMENT "Miles per Hour",
	gust_speed FLOAT COMMENT "Miles per Hour",
	precipitation FLOAT COMMENT "Inches",
	events VARCHAR(255) COMMENT "Free Text",
	conditions VARCHAR(255) COMMENT "Free Text",
	wind_direction_deg SMALLINT COMMENT "Degrees"
);
