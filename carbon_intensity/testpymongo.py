import pymongo
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as fc
from pyspark import SparkContext
import pandas as pd

def printf(p):
    print(list(p))

client = pymongo.MongoClient("localhost", 27017)
db = client['carbon_intensity']
collection=db['projectdatasets']
x=collection.find_one()
df=pd.DataFrame(x['data'])
# print(df)
df1=df['intensity'].apply(pd.Series)
df=pd.concat([df.drop(['intensity'],axis=1),df1],axis=1)
# print(df)
# ['forecast']

# # rowData=map(lambda x:Row(*x),x['data'])
# columns=['from_time','intensity','to_time']
spark = SparkSession.builder.getOrCreate()
df1=spark.createDataFrame(df)
df1.show(10)


# df=SparkSession.createDataFrame(columns,x['data'])

# print(type(df))
# print(type(x['data']))  #<class 'list'>
# print(x['data'][0])
# print(type(x['data'][0]))  #<class 'dict'>
# df = spark.createDataFrame(x)
# df.show(5)