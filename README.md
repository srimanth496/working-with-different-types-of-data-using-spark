# working-with-different-types-of-data-using-spark

creating spark session and read file from the system
![image](https://github.com/srimanth496/working-with-different-types-of-data-using-spark/assets/84217751/35956bbb-d491-4de3-a87a-d102822ae7cc)
df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("C:/Users/srima/data/retail-data/by-day/2010-12-01.csv")
creating the table from the data frame and perform sql operations on the table
df.createOrReplaceTempView("dfTable")

. import lit function for create new column in the table
from pyspark.sql.functions import lit

boolean condition is used for to check the condition
from pyspark.sql.functions import col
df.where(col("InvoiceNo") ==536365).select("InvoiceNo", "Description").show(5, False)
display the invoice with same invoice records others not print.
![image](https://github.com/srimanth496/working-with-different-types-of-data-using-spark/assets/84217751/92601329-0331-4879-89f1-1525509ffc42)

working with the strings in spark 

![image](https://github.com/srimanth496/working-with-different-types-of-data-using-spark/assets/84217751/3eda70a7-93e0-4b19-b60c-91f883d848f0)

trim the strings by using 
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
ltrim(lit("  HELLO  ")).alias("ltrim"),
rtrim(lit("  HELLO  ")).alias("rtrim"),
trim(lit("  HELLO  ")).alias("trim"),
lpad(lit("HELLO"), 3, " ").alias("lp"),
rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)

![image](https://github.com/srimanth496/working-with-different-types-of-data-using-spark/assets/84217751/09ac6bae-1008-42f9-8fa9-2a3ac3303357)

working with the date and time formate stamps
applying date and timestamp formate for 10 rows

from pyspark.sql.functions import current_date, current_timestamp
dateDF = spark.range(10)\
	.withColumn("today", current_date())\
	.withColumn("now", current_timestamp())

dateDF.createOrReplaceTempView("dateTable")

dateDF.printSchema()
![image](https://github.com/srimanth496/working-with-different-types-of-data-using-spark/assets/84217751/f2933f03-2992-418e-8ec8-1783df5afc6c)






