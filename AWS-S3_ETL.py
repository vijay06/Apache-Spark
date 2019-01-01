from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark.sql import SparkSession

if __name__ == "__main__":
	spark = SparkSession.builder.master("local").config(conf=SparkConf()).getOrCreate()

	csvDf = spark.read.csv("s3a://AKIAJGBLwdfrJZEDZIKO3H2A:5omtR46e2f3bSMIuGmel2K5hRulR7OIgrqy4Q4jFYcjWLuOm@pysparks3/Churn_Modelling.csv",header=True)

	csvDf.createOrReplaceTempView("Customer")

	sqlDF = spark.sql("SELECT * FROM Customer where age > 30")

	sqlDF.write.parquet("Customer.parquet",mode="overwrite")

	sqlDF.write.parquet("s3a://AKIAJGBLwdfrJZEDZIKO3H2A:5omtR46e2f3bSMIuGmel2K5hRulR7OIgrqy4Q4jFYcjWLuOm@pysparks3/test.parquet",mode="overwrite")

	sqlDF.show()

	spark.stop()



