# Spatial Data

Given a trajectory dataset, there is a method to 
- plot the trajectory count w.r.t length (bins). Gives you an idea about average length of trajectories.
- plot the trajectory count w.r.t duration (bins). Gives you an idea about average duration of trajectories.

# Fields of a trip dataset
* trip_id: A trip's unique identifier,
* mode:The mode of travel (0=Walk, 1=Vehicle, 2=Unknown),
* end_date: The trip's end date and time in UTC, ISO-8601 format, example: "2014-04-01T08:33:35.000"Z,
* end_dow: The trip's end weekday in local time, where 1=Mon, 2=Tues, 3=Wed, 4=Thurs, 5=Fri, 6=Sat, 7=Su,
* end_lat: The latitude coordinates of the trip's end point in decimal degrees,
* end_lon: The decimal degree longitude coordinates of the trip's end point in decimal degree,
* trip_distance_m: Trip distance in meters,
* vehicle_weight_class: Numeral representing the vehicle weight class

# Fields of waypoint dataset
* w_trip_id: A trip's unique identifier,
* waypoint_seq: The order of the waypoint within the trip starting with "1" and incrementing by one,
* capture_time: The capture date and time of the waypoint in UTC, ISO-8601 format, example: "2014-04-01T08:33:35.000Z",
* lat: The decimal degree latitude coordinates of the waypoint,
* lon: The decimal degree longitude coordinates of the waypoint,
* raw_speed: Raw speed,
* raw_speed_metric: Raw speed metric


# Objective:
- define bins boundaries for length: 5, 10, 20, 30, 50, 100, > 100 
- length of trajectories (count the number of waypoints)
- Number of trajectories in bins [min, max) w.r.t length
- plot lemgth bins VS number
- define bouderies for duration in minutes: 1, 5, 10, 15, 30, 60, > 60 
- Number of trajectories in bins [min, max) w.r.t duration
- plot duration bins VS number



1. File "data_processing_for_features.py" is a script with functions for processing of spatio temporal data and feature extraction. Before each functions, the parameters its need is written with sufficient explanation and examples.

2. "extractEventBasedonCity.py" is based on spark data preprocessing pipeline due to large input file size. It deals with extracting events for particular location (e.g. city: New York) from RDF event data. The input files are: zipcode of the city and event rdf dataset. Expected ouput: A csv file in RDF format with event name, event place's address and event time.

3. "getWeatherData_locationbased.py" deals with extraction of weather features for a particular city with a particular date and time span. Output: Weather features like temperature, pressure, wind speed etc. are extracted for particular location for the given time period (e.g. 1 year)

4. 
