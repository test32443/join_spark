#To run:
#spark-submit --packages com.databricks:spark-csv_2.10:1.2.0 spark_join.py 

from pyspark import SQLContext, SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType

conf = SparkConf().setAppName("spark_join").setMaster("local")
sc = SparkContext(conf=conf)
sql_sc = SQLContext(sc)

SalesSchema = StructType([
  StructField("TS",StringType()),
  StructField("cust_id",IntegerType()),
  StructField("price",DoubleType())])

sales = (sql_sc.read
  .format("com.databricks.spark.csv")
  .schema(SalesSchema)
  .load("sales.csv"))

CustSchema = StructType([
  StructField("cust_id",IntegerType()),
  StructField("Name",StringType()),
  StructField("Address",StringType()),
  StructField("City",StringType()),
  StructField("State",StringType()),
  StructField("Zip",IntegerType()) ])
  
cust = (sql_sc.read
  .format("com.databricks.spark.csv")
  .schema(CustSchema)
  .load("customers.csv"))

result = cust.join(sales,sales.cust_id == cust.cust_id).groupBy("State").sum("price")

# Same logic but using SparkSQL
#df.registerTempTable("sales")
#cust.registerTempTable("customers")         
#result = sql_sc.sql("select c.C4 as state,sum(s.C2) from customers c inner join sales s on s.C1 = c.C0 group by c.C4")

result.coalesce(1).write.format("com.databricks.spark.csv").save("results.csv")

