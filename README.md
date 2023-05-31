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

Map matching for car trajectory data

1. Fast Map matching
   1. As the name depicts, fast map matching has a better computational complexity (run time), which means it has achieved a considerable single-processor map matching speed of 25,000–45,000 points/second [1]. 
   2. The steps for installing FMM are available at https://fmm-wiki.github.io/ 
   3. The Jupyter notebooks are also available for the same https://github.com/cyang-kth/fmm/tree/master/example/notebook 


2. Valhalla Map Matching
   1. First, a docker needs to be installed on the system (https://docs.docker.com/engine/install/ubuntu/ )
   2. This link describes the step to install a map matching algorithm named “Valhalla” in Ubuntu by using docker (https://gis-ops.com/de/valhalla-how-to-run-with-docker-on-ubuntu/). 
   3. Once Valhalla is installed on your system using the above methods, the following repository code will help perform the map matching steps.
https://github.com/zotttttttt/gps-trace-optimization/blob/main/GPS-trace-optimization-via-Valhalla.ipynb 


# Different files details

1. File "data_processing_for_features.py" is a script with functions for processing of spatio temporal data and feature extraction. Before each functions, the parameters its need is written with sufficient explanation and examples.

2. "extractEventBasedonCity.py" is based on spark data preprocessing pipeline due to large input file size. It deals with extracting events for particular location (e.g. city: New York) from RDF event data. The input files are: zipcode of the city and event rdf dataset. Expected ouput: A csv file in RDF format with event name, event place's address and event time.

3. "getWeatherData_locationbased.py" deals with extraction of weather features for a particular city with a particular date and time span. Output: Weather features like temperature, pressure, wind speed etc. are extracted for particular location for the given time period (e.g. 1 year)
