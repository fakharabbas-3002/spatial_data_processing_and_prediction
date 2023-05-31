# Spatial Data

1. File "data_processing_for_features.py" is a script with functions for processing of spatio temporal data and feature extraction. Before each functions, the parameters its need is written with sufficient explanation and examples.

2. "extractEventBasedonCity.py" is based on spark data preprocessing pipeline due to large input file size. It deals with extracting events for particular location (e.g. city: New York) from RDF event data. The input files are: zipcode of the city and event rdf dataset. Expected ouput: A csv file in RDF format with event name, event place's address and event time.
