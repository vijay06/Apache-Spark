from pyspark.sql import SparkSession
from pyspark import  SparkConf
from pyspark.sql import SQLContext

from pyspark import SparkContext
sc = SparkContext("local", "sqlIntegration")

# Create a SparkSession
spark = SparkSession.builder.appName("sqlIntegration").config("spark.some.config.option").getOrCreate()


# Read it  from mysql into a new Dataframe
dataset=spark.read.format("jdbc").options(url ="jdbc:mysql://localhost/mysql",driver="com.mysql.jdbc.Driver",dbtable="office.emp",user="root",password="root").load().take(10)


#List into Dataframe
dataset = sc.parallelize(dataset).toDF()

#create table by using createReplaceTempView.
dataset.createOrReplaceTempView("emp")

sqlDF = spark.sql("SELECT 6 as id ,'vj'as name ,age,salary+2000 as salary FROM emp WHERE id=1")


# Write it into mysql

sqlDF.write.format('jdbc').options(
      url='jdbc:mysql://localhost/mysql',
      driver='com.mysql.jdbc.Driver',
      dbtable='office.emp',
      user='root',
      password='root').mode('append').save()
