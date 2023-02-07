from flask import Flask, render_template, request
from pyspark.sql import SparkSession
import pyspark.sql.functions as fc
import pandas as pd

app = Flask(__name__)


spark = SparkSession.builder.appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/sample1.zips") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/sample1.zips") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()
df1 = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
    .option("database", "carbon-intensity") \
    .option("collection", "projectdatasets").load()
df3 = df1.select(fc.explode(fc.col('data.from')).alias('from_time'))
df4 = df1.select(fc.explode(fc.col('data.to')).alias('to_time'))
df2 = df1.select(fc.expr('inline(data.intensity)'))
pandas_df = pd.concat([df3.toPandas(), df4.toPandas(), df2.toPandas()], axis=1)
spark_df = spark.createDataFrame(pandas_df)

df_rdd=spark_df.rdd.repartition(3)
index_list=['very low','low','moderate','high','very high']


def search_by_input(input):
    rdd_temp=df_rdd.filter(lambda x:x[0][:-1]>=input[0])
    rdd_temp=rdd_temp.filter(lambda x:x[1][:-1]<=input[1])
    if input[5]=='1' and input[2]!='':
        rdd_temp=rdd_temp.filter(lambda x:x[2]==int(input[2]))
    elif input[5]=='0' and input[3]!='' and input[4]!='':
        rdd_temp = rdd_temp.filter(lambda x: x[2] >= int(input[3]) and x[2] <= int(input[4]))
    elif input[5] == '0' and input[3] != '':
        rdd_temp = rdd_temp.filter(lambda x: x[2] >= int(input[3]))
    elif input[5] == '0' and input[4] != '':
        rdd_temp = rdd_temp.filter(lambda x: x[2] <= int(input[4]))
    elif input[9] == '1' and input[6] != '':
        rdd_temp = rdd_temp.filter(lambda x: x[3] == int(input[6]))
    elif input[9] == '0' and input[7] != '' and input[8] != '':
        rdd_temp = rdd_temp.filter(lambda x: x[3] >= int(input[7]) and x[3] <= int(input[8]))
    elif input[9] == '0' and input[7] != '':
        rdd_temp = rdd_temp.filter(lambda x: x[3] >= int(input[7]))
    elif input[9] == '0' and input[8] != '':
        rdd_temp = rdd_temp.filter(lambda x: x[3] <= int(input[8]))
    if input[10]!='':
        rdd_temp = rdd_temp.filter(lambda x: x[4] == index_list[int(input[10])])

    print(rdd_temp.count())
    return rdd_temp


@app.route('/home')
def home():
    return render_template("home.html")


@app.route('/search', methods=['POST', 'GET'])
def search():
    rdd_result=[]
    if request.method == 'POST':
        input_list=[]
        input_list.append(request.values.get('from'))
        input_list.append(request.values.get('to'))
        input_list.append(request.values.get('exact-predict'))
        input_list.append(request.values.get('begin-predict'))
        input_list.append(request.values.get('finish-predict'))
        input_list.append(request.values.get('predictchoice'))
        input_list.append(request.values.get('exact-actual'))
        input_list.append(request.values.get('begin-actual'))
        input_list.append(request.values.get('finish-actal'))
        input_list.append(request.values.get('actualchoice'))
        input_list.append(request.values.get('index'))
        c_name=request.values.get('upload')

        print(input_list)

        rdd_result=search_by_input(input_list)
        # print(type(rdd_result.collect()))  #<class 'list'>
        # print(type(rdd_result.collect()[0]))  #<class 'pyspark.sql.types.Row'>
        result=rdd_result.sortBy(lambda x: x[0]).collect()
        if c_name!='':
            redf=spark.createDataFrame(rdd_result)
            redf.write.format('com.mongodb.spark.sql.DefaultSource') \
                .option("database", "carbon-intensity") \
                .option("collection", c_name).mode('overwrite').save()
            print('success')

        return render_template("search.html", result=result)
    else:
        print('else')
        return render_template("search.html")

@app.route('/analysis')
def analysis():
    month=['2020-01','2020-02','2020-03','2020-04','2020-05','2020-06',
           '2020-07','2020-08','2020-09','2020-10','2020-11','2020-12','2021-01']
    list_all=[[],[],[],[],[]]
    for m in month:
        rdd_mon = df_rdd.filter(lambda x: x[0][:7] == m)
        print(m)
        list_all[0].append(rdd_mon.filter(lambda x: x[4] == index_list[0]).count())
        list_all[1].append(rdd_mon.filter(lambda x: x[4] == index_list[1]).count())
        list_all[2].append(rdd_mon.filter(lambda x: x[4] == index_list[2]).count())
        list_all[3].append(rdd_mon.filter(lambda x: x[4] == index_list[3]).count())
        list_all[4].append(rdd_mon.filter(lambda x: x[4] == index_list[4]).count())
    print('filter done')
    rdd_tem=df_rdd.filter(lambda x: x[0][:9] == '2020-01-0').sortBy(lambda x: x[0])
    predict_all=rdd_tem.map(lambda x:x[2]).collect()
    print('predict done')
    actual_all=rdd_tem.map(lambda x:x[3]).collect()
    print('actual done')
    time_all=rdd_tem.map(lambda x:x[0][:-1]).collect()
    return render_template("analysis.html", x=month, y=list_all, t=time_all, i=predict_all, j=actual_all)


if __name__ == '__main__':
    app.run()
