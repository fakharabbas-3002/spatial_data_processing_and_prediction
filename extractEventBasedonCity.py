from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.types as types
import pandas as pd
import argparse
from rdflib import ConjunctiveGraph
from pyspark.sql.functions import *
from functools import reduce
import pyspark.sql.functions as f

# set up spark context
spark = SparkSession. \
    builder. \
    appName('event_data_city').\
    config('spark.driver.memory', '25g').\
     config('spark.dirver.maxResultSize', '12g').getOrCreate()
    

sc = spark.sparkContext
# Download event file from:  http://data.dws.informatik.uni-mannheim.de/structureddata/2021-12/quads/classspecific/schema_Event/
paths=["part_11"]
for path in paths:
    print(path)
    df = spark.read.text(path)
    df.printSchema()

    df= df.withColumn("tmp",split(col("value"),">")).\
    withColumn("product1",element_at(col("tmp"),1)).\
    withColumn("product2",element_at(col("tmp"),2)).\
    withColumn("product3",coalesce(element_at(col("tmp"),3),lit(""))).drop("tmp")
    df=df.drop("value")

    df=df.drop("value")
    # coverting rdf data to s,p,o
    df= df.withColumn("tmp",split(col("product1")," ")).\
    withColumn("product1_1",element_at(col("tmp"),1)).\
    withColumn("product1_2",coalesce(element_at(col("tmp"),2),lit(""))).drop("tmp")
    
    df=df.drop("product1")
    df = df.select("product1_1","product1_2","product2")
    
    #filtering based on country-US
    df1=df.filter((df.product1_2  == "<http://schema.org/addressCountry"))
    #df1.show()

    #array of postal code
    # Event extraction based on postal code of a New York city
    postcode_NYC=pd.read_csv("NYC_postcode.csv")
    postcode_NYC=postcode_NYC.drop(columns=["Unnamed: 0","Unnamed: 1"])
    postcode_array=postcode_NYC['Code'].values
    A = []
    for x in postcode_array:
        A.append(str(x))
    
    #country with US filter out 
    df2=df1.withColumn("tmp",split(col("product2")," <")).\
    withColumn("country",element_at(col("tmp"),1)).\
    withColumn("country_uri",coalesce(element_at(col("tmp"),2),lit(""))).drop("tmp").drop("product2")

    df_US1=df2.filter(df2.country.contains('US'))
    df_US1.show()
    
    US_array = df_US1.select("product1_1").rdd.flatMap(lambda x: x).collect()
    US_array=set(US_array)
  
    df_US_address=df.filter((col("product1_1").isin(US_array)))
   
    df_postalCode=df_US_address.filter(df_US_address.product1_2.contains('<http://schema.org/postalCode'))

    
    ### filtering postacode of city- NY
    df_postalCode=df_postalCode.withColumn("tmp",split(col("product2"),"<")).\
    withColumn("postcode",element_at(col("tmp"),1)).\
    withColumn("postcode_uri",coalesce(element_at(col("tmp"),2),lit(""))).drop("tmp")

    
    df_postalCode=df_postalCode.withColumn('postcode', regexp_replace(col("postcode"), " ", ""))

    df_postalCode = df_postalCode.withColumn('postcode', regexp_replace('postcode', '"', ''))

    df_required_postalCode=df_postalCode.where(f.col("postcode").isin(A))

    #|_:n680cb1814a9941adaafb9c7eec668b6cxb2 |<http://schema.org/postalCode>|"7000"
    postcodeAdress_array = df_required_postalCode.select("product1_1").rdd.flatMap(lambda x: x).collect()
    postcodeAdress_set=set(postcodeAdress_array)
    #print(postcodeAdress_set)
    
    # filtering whole address to combine it later with event
    df6=df.filter(col("product1_1").isin(postcodeAdress_set))
    #print("Filtering address subject")
    #df6.show(5,False)
    
    #postcodeAdress_set
    postcodeAdress_set_array=list(postcodeAdress_set)
        
    df24=df.withColumn("tmp",split(col("product2")," <")).\
    withColumn("object",element_at(col("tmp"),1)).\
    withColumn("object_uri",coalesce(element_at(col("tmp"),2),lit(""))).drop("tmp").drop("product2")
    df_object=df24.withColumn('object', regexp_replace(col("object"), " ", ""))

 
    df21=df_object.filter(col("object").isin(postcodeAdress_set_array))
    df22 = df21.select("product1_1","product1_2","object")
    df22.show()
     #_:n680cb1814a9941adaafb9c7eec668b6cxb1 |<http://schema.org/address>|_:n680cb1814a9941adaafb9c7eec668b6cxb2

    address_array = df21.select("product1_1").rdd.flatMap(lambda x: x).collect()
     df3=df_object.filter(col("object").isin(address_array))
      
    print("Filtering location subject")
    #df3.show(5,False)
    #_:n680cb1814a9941adaafb9c7eec668b6cxb0 |<http://schema.org/location>|_:n680cb1814a9941adaafb9c7eec668b6cxb1

    #df_union12_3=df_union1_2.union(df3)

    event_array = df3.select("product1_1").rdd.flatMap(lambda x: x).collect()

    df4=df.filter(col("product1_1").isin(event_array))
    #|_:n680cb1814a9941adaafb9c7eec668b6cxb0|<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>|<http://schema.org/Event>                                        |
    #|_:n680cb1814a9941adaafb9c7eec668b6cxb0|<http://schema.org/description>                  |"&lt;p&gt;Der                                                    |
    #|_:n680cb1814a9941adaafb9c7eec668b6cxb0|<http://schema.org/endDate>                      |"2021-11-05T23:00:00+01:00"^^<http://schema.org/Date> 

    df_all=df6.union(df4)
    df_all_21=df_all.union(df22)
    
    df_all_21.coalesce(1).write.option("delimiter","µ").csv("datacsvtest"+path)
