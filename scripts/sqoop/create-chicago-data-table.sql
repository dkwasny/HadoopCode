CREATE TABLE IF NOT EXISTS data(
	zipcode INTEGER,
	datetime DATETIME COMMENT "UTC time in ISO format",
	temperature FLOAT COMMENT "Fahrenheit",
	dew_point FLOAT COMMENT "Fahrenheit",
	humidity INTEGER,
	sea_level FLOAT COMMENT "Inches of Mercury",
	visibility FLOAT COMMENT "Miles",
	wind_direction VARCHAR(255) COMMENT "Cardinal Direction",
	wind_speed FLOAT COMMENT "Miles per Hour",
	gust_speed FLOAT COMMENT "Miles per Hour",
	precipitation FLOAT COMMENT "Inches",
	events VARCHAR(255) COMMENT "Free Text",
	conditions VARCHAR(255) COMMENT "Free Text",
	wind_direction_deg INTEGER COMMENT "Degrees",

	PRIMARY KEY (zipcode, datetime)
);
