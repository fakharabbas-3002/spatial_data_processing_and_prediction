### Script with functions for Spatial Data Processing
### Authors: Uttam Kumar


import pandas as pd
import numpy as np
import regex as re
from scipy.stats import zscore
import math
import time
import geopandas
import pygeohash as pgh
from collections import Counter
import datetime
from scipy import stats
import pyproj
from pyproj import Transformer, CRS
from sklearn.neural_network import MLPClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn import svm

# Drop rows where for some columns NaN values are less than a threshold say 0.5%
# lst_of_columns_to_be_dropped = ['City','Zipcode','Airport_Code', 'Sunrise_Sunset','Civil_Twilight','Nautical_Twilight','Astronomical_Twilight']
def drop_rows_wit_col_1pct_NaN(df, lst_of_columns_to_be_dropped):
    try:
       df = df.dropna(subset=lst_of_columns)
    except Exception as ex:
       print('Exception while dropping rows for attributes with 1 percent NaN values in feature extraction script')
       print(ex)
       return df
       pass
    return df 



# function to convert date_time field to pandas date time and form new date related features like eventDuration,day,month,year,dayOfWeek
# Start_Time is the field name of start timestamp as given in dataset
# End_Time is the field name of end timestamp as given in dataset
def get_time_features(df, Start_Time, End_Time):
    print("adding time features ...")
    try:
       st = time.time()
       df["startTime"] = pd.to_datetime(df[Start_Time])
       df["endTime"] = pd.to_datetime(df[End_Time])
       df["eventDuration"] = round((df["endTime"] - df["startTime"]).dt.total_seconds() / 60)
       #df["accDuration"] = df.apply(lambda row: (pd.to_datetime(row['End_Time']) - pd.to_datetime(row['Start_Time'])).seconds / 60, axis=1)
       print('eventDuration added to the df in '+str(round(time.time()-st))+' sec')
       df['day'] = pd.to_datetime(df[Start_Time]).dt.day  
       df['month'] = pd.to_datetime(df[Start_Time]).dt.month
       df['year'] = pd.to_datetime(df[Start_Time]).dt.year
       df['dayOfWeek'] = pd.to_datetime(df[Start_Time]).dt.dayofweek  # 0 till 6
       print('eventDuration,day,month,year,dayOfWeek added to the df in '+str(round(time.time()-st))+' sec')
    except Exception as ex:
       print('Exception while adding geo-time features in feature_extraction script')
       print(ex)
       return df
       pass
    return df
    
    

#Filling in missing values of categorical weather features with their majority values rather than median
def fill_values_with_majority(df,lst_col_to_be_grouped_by_majority,lst_col_criteria):
    # group data by 'Airport_Code' and 'month' then fill NAs with majority value
    # lst_col_to_be_grouped_by_majority = ['Wind_Direction', 'clear', 'cloud', 'rain', 'heavyRain', 'snow', 'heavySnow', 'fog']
    # lst_col_criteria = ['Airport_Code','month']
    try:
       for i in lst_col_to_be_grouped_by_majority:
          df[i] = df.groupby(lst_col_criteria)[i].apply(lambda x: x.fillna(Counter(x).most_common()[0][0]) if all(x.isnull())==False else x)
          print(i + " : " + df[i].isnull().sum().astype(str))
       # drop na
       df = df.dropna(subset=weather_cat)
    except Exception as ex:
       print('Exception while filling in categorical values with majority in feature extraction script')
       print(ex)
       return df
       pass
    return df 
    

#Add a new feature indicating missing values in 'Precipitation_in' and impute the missing values with median in original column.
# column_to_be_imputed e.g. Precipitation_in, derived_imputed_column e.g. precipitationNA
def fill_values_for_precipitation(df):
    try:
       df[derived_imputed_column] = 0
       df.loc[df[column_to_be_imputed].isnull(),derived_imputed_column] = 1
       df[column_to_be_imputed] = df[column_to_be_imputed].fillna(df[column_to_be_imputed].median())
    except Exception as ex:
       print('Exception while filling in values for '+column_to_be_imputed+' in feature extraction script')
       print(ex)
       return df
       pass
    return df
    

'''
Filling in some attributes(like Temperature_F, Humidity_pct, Pressure_in, Visibility_mi, Wind_Speed_mph) which have small missing part.
we group weather features by location and time first, to which weather is naturally related. 'Airport_Code' is selected as location feature because 
#the sources of weather data are airport-based weather stations. Then we group the data by 'month' rather than 'hour' because using 
#month its computationally cheaper and remains less missing values. Finally, missing values will be replaced by median value of each group.
'''
# group data by a list of columns (columns_group_by_with) say 'Airport_Code' and 'month' then impute NAs in columns_requiring_median_imputation with median value
def fill_values_with_median(df, columns_requiring_median_imputation, columns_group_by_with):
    try:
       # columns_requiring_median_imputation = ['Temperature_F','Humidity_pct','Pressure_in','Visibility_mi','Wind_Speed_mph']
       # columns_group_by_with = ['Airport_Code','month']
       print("The number of remaining missing values: ")
       for i in columns_requiring_median_imputation:
          df[i] = df.groupby([columns_group_by_with])[i].apply(lambda x: x.fillna(x.median()))
          print( i + " : " + df[i].isnull().sum().astype(str))
       #still if there are some missing values but much less, then just dropna by these features for simplicity
       df = df.dropna(subset=columns_requiring_median_imputation)
    except Exception as ex:
       print('Exception while filling in values with median in feature extraction script')
       print(ex)
       return df
       pass
    return df 
    
    
#function to drop columns from a data-frame which have either single value like Country and Turning_Loop or those with > a threshold say 60% values missing like Wind_Chill and Number
def drop_unwanted_columns(df, columns_to_be_dropped):
    # columns_to_be_dropped = ['Country','Turning_Loop','Wind_Chill_F','Number']
    try:
       df = df.drop(columns_to_be_dropped, axis=1)
    except Exception as ex:
       print('Exception while dropping unwanted columns in feature_extraction script')
       print(ex)
       return df
       pass
    return df
    
    



# function to convert boolean fields having values as 'True' or 'False' to 1 or 0 as per its value of True or False
def convert_boolean_features(df, columns_for_bool_to_num_conv):
    #Converts following 13 columns
    #'Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway', 'Roundabout', 'Station','Stop','Traffic_Calming','Traffic_Signal','Turning_Loop'
    print("converting boolean features to 1 or 0 ...")
    try:
       # columns_for_bool_to_num_conv = ['Amenity','Bump','Crossing','Give_Way','Junction','No_Exit','Railway','Roundabout','Station','Stop','Traffic_Calming','Traffic_Signal','Turning_Loop']
       for col in columns_for_bool_to_num_conv:
           df[col] = df[col].astype(int)
    except Exception as ex:
       print('Exception while converting boolean features in feature_extraction script')
       print(ex)
       return df
       pass
    return df




# function to add geometry and geohashes to an existing dataframe 
# geohash 5 means 4.89*4.89 KM which we use for our prediction, +we also add geohash 4 and 6 for future
def add_geohashes(df, column_lat, column_long):
    try:
       # column_lat, column_long = 'Start_Lat', 'Start_Lng'
       st=time.time()
       print("adding geohashes...")
       #making a geoDataframe by adding geometry
       df = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df.Start_Lng, df.Start_Lat))
       #adding geohashes with different precision
       for i in range(3):
           k = i+4
           geo_str = 'geohash'+str(k)
           df[geo_str]=df.apply(lambda row: pgh.encode(row[column_lat], row[column_long], k), axis=1)
       print('geometry, geohash4 to geohash6 added to the df in '+str(round(time.time()-st))+' sec')
    except Exception as ex:
       print('Exception while computing geohash in feature extraction')
       print(ex)
       return df
       pass
    return df



# function to add columns with boolean values to a df where column names should be same as elements in unique list of existing values in one categorical column(column_to_be_boolified) 
def add_boolean_wind_direction(df, column_to_be_boolified):
    try:
       # column_to_be_boolified = 'Wind_Direction'
       wind_dir_val_list = list(df.column_to_be_boolified.unique())
       for i in wind_dir_val_list:
           wd = 'windDirection'+str(i)
           df[wd] = df[column_to_be_boolified].apply(lambda x: 1 if x.strip()==i else 0)
       print('{} columns with boolean values added for {}'.format(str(len(wind_dir_val_list)), column_to_be_boolified))
    except Exception as ex:
       print('Exception while adding boolean values for '+ column_to_be_boolified +' in feature extraction script')
       print(ex)
       return df
       pass
    return df




# add time-bin boolean columns, each time bin being of number of minutes provided as input from program else consider it to be 15 minute, tot 1+96 colmns get added
def add_time_bins(df,m=15,column_startTime):
    try:
       # column_startTime = 'startTime'
       # 0.1 is added so that 00:00:00 hr gets allotted to bin 1 instead of bin 0 which should not exist among allowed bins [1,2,....,95,96]
       df['timeQuarterOfDay'] = df[column_startTime].apply(lambda x: (math.ceil((((int(str(x).split(' ')[-1].split(':')[0]))*60)+0.1+(int(str(x).split(' ')[-1].split(':')[1])))/m))) 
       for i in range(96):
          quartNum = 'quarter'+str(i+1)
          df[quartNum] = df['timeQuarterOfDay'].apply(lambda x: 1 if x==(i+1) else 0)            
    except Exception as ex:
       print('Exception while adding timeQuarterOfDay and quartNum(1..96) in feature extraction script')
       print(ex)
       return df
       pass
    return df



#get log of a column
def get_col_log(df,col):
    try:
       # col = 'order'
       df['log_'+str(col)]= df[col].apply(lambda x: np.log10(x) if x!=0 else 0)  
       #df.drop(col, axis=1, inplace=True)
       #df.rename(columns={'log'+str(col):col}, inplace=True)
    except Exception as ex:
       print('Exception while adding log of a column in feature extraction script')
       print(ex)
       return df
       pass
    return df



#get anti-log of a column
def get_col_antiLog(df,col):
    try:
       # col = 'order'
       df['antiLog_'+str(col)]= df[col].apply(lambda x: int(round(10**x)) if x!=0 else 0)
       #df.drop(col, axis=1, inplace=True)
       #df.rename(columns={'antiLog'+str(col):col}, inplace=True)
    except Exception as ex:
       print('Exception while adding anti-log of a column in feature extraction script')
       print(ex)
       return df
       pass
    return df


#function to get Weekday or Weekend; 0 to 4 is Monday to Friday
def get_WeekdayWeekend(df):
    try:
       df['weekday'] = df['dayOfWeek'].apply(lambda x: 0 if (x==5 or x==6) else 1)
       print('weekday column added having boolean values')
    except Exception as ex:
       print('Exception while adding boolean valued column weekday in feature extraction script')
       print(ex)
       return df
       pass
    return df

# Outlier Removal: For multiple columns in df if we need to remove all rows that have outliers in at least one column
def fetch_df_without_outliers(df):
    col_len = len(df.columns)
    return df[(np.abs(stats.zscore(df)) < col_len).all(axis=1)]


## Trajectory points interpolation related 2 functions follows
# method to convert from spherical to planar coordinates
def fetch_planar_coord(lat, long, transformer):
    if np.isnan(lat) or np.isnan(long):
        x,y = np.nan, np.nan    
    else:
        x,y = transformer.transform(lat,long)
    concat_xy = str(x)+','+str(y)
    return concat_xy

def impute_df(df):
    df['x'] = 0
    df['y'] = 0
    df['concat_xy'] = ""
    transformer = Transformer.from_crs(4326, 27700) # transformer to perform Step 1 of solution
    back_transformer = Transformer.from_crs(27700, 4326) # back-transformer to perform Step 4 of solution
    
    
    df['concat_xy'] = df.apply(lambda row: fetch_planar_coord(row['latitude'], row['longitude'], transformer), axis=1) #indexes are not counted and numbering starts from 0, -1 means the last column
    df['x'] = df['concat_xy'].apply(lambda x: float(x.split(',')[0]) if x.split(',')[0] != 'nan' else np.nan)
    df['y'] = df['concat_xy'].apply(lambda y: float(y.split(',')[1]) if y.split(',')[1] != 'nan' else np.nan)   
    df = df[['latitude','longitude','x','y']]
    
    ## Step 3: interpolating missing values in planar coordinates
    df = df.interpolate(method='linear')
    
    
    ## Step 4: reconverting to spherical coordinates
    lat_back, lon_back = back_transformer.transform(df.x.values, df.y.values)
    df.latitude = lat_back
    df.longitude = lon_back
    
    df = df[['latitude','longitude']]
    
    return df


''''
This method fills missing class values with classification methods.
Input: DataFrame
Parameter: You can select between different sklearn classification models 
model= [1=MLPClassifier, 2=LogisticRegression, 3=kNeighbirsClassifier, 4=SVM]
modelparameters= Depending on the selected model you can select modelparameters Default: ""
value_for_unknown_labels= How are the unknown labels described? In default case with the String "unknown"
variable_name= Variable name of the classes/labels
Output: DataFrame
'''

def fill_label_gaps_with_classification(df, model=1, modelparameters ="", value_for_unknown_labels ="unknown",
                                        variable_name = "class"):
    if model == 1:
        m = MLPClassifier(modelparameters)
    elif model == 2:
        m = LogisticRegression(modelparameters)
    elif model == 3:
        m = KNeighborsClassifier(modelparameters)
    elif model == 4:
        m = svm.SVC(modelparameters)

    unknownlabels = df[df.values == value_for_unknown_labels]
    traindata = df.copy(deep=True)
    traindata.drop(traindata[traindata.values == value_for_unknown_labels].index, inplace=True)

    # Labels are the values we want to predict
    trainlabels = np.array(traindata[variable_name], dtype=np.int64)
    traindata.drop([variable_name], axis=1, inplace=True)
    trainfeatures = np.array(traindata)

    unknownlabels.drop([variable_name], axis=1, inplace=True)
    datawithmissinglabels = np.array(unknownlabels)

    m.fit(trainfeatures, trainlabels)

    predictions = m.predict(datawithmissinglabels)
    i = 0
    for d in df.iloc:
        if value_for_unknown_labels == d[variable_name]:
            d[variable_name] = predictions[i]
            i += 1

    return df
