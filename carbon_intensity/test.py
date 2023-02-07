
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as fc
import pandas as pd


def map_to_pandas(rdds):
    return [pd.DataFrame(list(rdds))]

def partition_collect(df):
    df = df.repartition(2)
    df_p=df.rdd.mapPartitions(map_to_pandas).collect()
    df_p = pd.concat(df_p)
    df_p.columns=df.columns
    return df_p

def connect_mongo():
    spark = SparkSession.builder.appName("myApp") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/sample1.zips") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/sample1.zips") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()
    df1 = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", "carbon-intensity") \
        .option("collection", "projectdatasets").load()
    df3 = df1.select(fc.explode(fc.col('data.from')).alias('from_time'))  # 会得到竖着的pandas
    df4 = df1.select(fc.explode(fc.col('data.to')).alias('to_time'))  # 会得到竖着的pandas
    df2 = df1.select(fc.expr('inline(data.intensity)'))  # intensity 三列
    pandas_df = pd.concat([df3.toPandas(), df4.toPandas(), df2.toPandas()], axis=1)
    spark_df = spark.createDataFrame(pandas_df)
    return spark_df

def a():
    spark = SparkSession.builder.appName("myApp") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/sample1.zips") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/sample1.zips") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .config('spark.executor.memory', '4g') \
        .getOrCreate()

    # read mongodb
    # df1.filter(df1['test1']=='test1').show()

    df1 = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", "carbon-intensity") \
        .option("collection", "projectdatasets") \
        .load()

    # df2=df1.select(df1['data.from'],df1['data.to'],fc.explode(df1['data.intensity'])).toDF('from_time','to_time','intensity')
    # df2=df1.select(fc.expr(fc.col('data.from')),fc.expr(fc.col('data.to')),fc.expr(fc.col('data.intensity.forecast')),fc.expr(fc.col('data.intensity.actual')),fc.expr(fc.col('data.intensity.index')))

# +-----------------+-----------------+--------+------+--------+
# | from_time | to_time | forecast | actual | index |
# +-----------------+-----------------+--------+------+--------+
    df3=df1.select(fc.explode(fc.col('data.from')).alias('from_time'))  #会得到竖着的pandas
    df4=df1.select(fc.explode(fc.col('data.to')).alias('to_time'))  #会得到竖着的pandas
    df2=df1.select(fc.expr('inline(data.intensity)'))  #intensity 三列
    # df2=df1[['data.from']]  #会得到横着的pandas
    pandas_df=pd.concat([df3.toPandas(),df4.toPandas(),df2.toPandas()],axis=1)
    spark_df=spark.createDataFrame(pandas_df)
    spark_df.show(10)
    # df2=spark_df.filter(spark_df['forecast'] == 170)
    # print(df2.count())


#得到无序的
    # df3=df1.select(fc.explode(fc.col('data.from')).alias('from_time'))  #会得到竖着的pandas
    # df4=df1.select(fc.explode(fc.col('data.to')).alias('to_time'))  #会得到竖着的pandas
    # df2=df1.select(fc.expr('inline(data.intensity)'))  #intensity 三列
    # # df2=df1[['data.from']]  #会得到横着的pandas
    # a=pd.concat([partition_collect(df3),partition_collect(df4),partition_collect(df2)],axis=1)
    # print(a)

    # ----------------------------------------
    # df1.withColumn('from_time',fc.explode(fc.col('data.from')).alias('from_time'))
    # df3=df1.select(fc.explode(fc.col('data.from')).alias('from_time'))
    # df4=df1.select(fc.explode(fc.col('data.to')).alias('to_time'))  #会得到竖着的pandas


    # df2=df1.select(fc.expr('inline(data.intensity)'))  #intensity 三列
    # df2=df1[['data.from']]  #会得到横着的pandas
    # a=pd.concat([df3.toPandas(),df4.toPandas(),df2.toPandas()],axis=1)

    # d=fc.explode(fc.col('data.from'))#<class 'pyspark.sql.column.Column'>
    # d=fc.col('data.from')#<class 'pyspark.sql.column.Column'>

    #------------------------------------------

    # df1.foreach(lambda x: print(x))
    # 废物没用
    # df2=df1.toJSON().values().flatMap(lambda x:x.replace('{','#!#').split('#!#'))
    # df2.foreach(lambda x: print(x))

    # # df1=df1['data.from','data.to','data.intensity.forecast','data.intensity.actual','data.intensity.index']
    # # df2=df1['data.from','data.to','data.intensity.forecast','data.intensity.actual','data.intensity.index'].toDF('from_time','to_time','forecast','actual','index')
    # # df1 = df1.withColumn(df1['data.from'].alias('from_time'), df1['data.to'].alias('to_time'), fc.expr('inline(data.intensity)'))
    # # df1 = df1.select(df1[['data.from']], df1[['data.to']].alias('to_time'), fc.expr('inline(data.intensity)'))
    # df1 = df1.select(df1[['data.from','data.to', fc.expr('inline(data.intensity)')]])
    # # df1 = df1[['data.from','data.to', fc.expr('inline(data.intensity)')]]
    # df1=df1.withColumn('from', fc.explode(fc.col('from')))
    # df1=df1.withColumn('to', fc.explode(fc.col('to')))
    # df1.show(10)
    # print(df1.count())

    # df1 = df1['data.from','data.to','data.intensity']
    # dfp=df1.toPandas()
# +--------------------+--------------------+--------------------+
# | from | to | intensity |
# +--------------------+--------------------+--------------------+
# | [2020 - 01 - 01T00: 00... | [2020 - 01 - 01T00:30... | [[178, 182, moder... |
# +--------------------+--------------------+--------------------+

    # df1 = df1.filter(df1['index'] == 'low' and df1['from'] like '%00:%')
    # df1 = df1.filter(df1.from_time.contains('00:'))

# 可以运行
#     a=df1.toPandas()
# a=df1.select('from').toPandas()['from']
# a=[int(row['from']) for row in df1.selectExpr('from').collect()]

# 可以运行
#     df1=df1.repartition(2)
#     df_p=df1.rdd.mapPartitions(map_to_pandas)
#     # print(type(df_p))
#     df_p=df1.rdd.mapPartitions(map_to_pandas).collect()
#     df_p=pd.concat(df_p)
#     df_p.columns=df1.columns
#     print(df_p)

# +--------------------+--------------------+--------+------+--------+
# |                from|                  to|forecast|actual|   index|
# +--------------------+--------------------+--------+------+--------+
# |[2020-01-01T00:00...|[2020-01-01T00:30...|     178|   182|moderate|


if __name__ == "__main__":
    # execute only if run as a script
    a()
