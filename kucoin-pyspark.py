
													
													Comparing statistics of two different online currencies


pyspark --jars /root/spark-sql-kafka-0-10_2.11-2.3.1.jar,/usr/hdp/current/kafka-broker/libs/kafka-clients-1.1.1.3.0.1.0-187.jar

from pyspark.streaming import StreamingContext
from pyspark.sql.functions import *
from pyspark.sql.functions import col,from_json,json_tuple
import pyspark.sql.functions as psf
from pyspark.sql.types import *


df=spark.readStream.format("kafka").option('kafka.bootstrap.servers','sandbox-hdp:6667').option('subscribe','lab').option('startingOffsets','earliest').load()
df.schema
df.printSchema()


schema=StructType([StructField('code',StringType(),True),StructField('data',StructType([StructField('averagePrice',StringType(),True),StructField('buy',StringType(),True),StructField('changePrice',StringType(),True),StructField('changeRate',StringType(),True),StructField('high',StringType(),True),StructField('last',StringType(),True),StructField('low',StringType(),True),StructField('makerCoefficient',StringType(),True),StructField('makerFeeRate',StringType(),True),StructField('sell',StringType(),True),StructField('symbol',StringType(),True),StructField('takerCoefficient',StringType(),True),StructField('takerFeeRate',StringType(),True),StructField('time',LongType(),True),StructField('vol',StringType(),True),StructField('volValue',StringType(),True)]),True)])


ds = df.writeStream.format("console").trigger(processingTime = '30 second').start() 
ds = df.select(col('value').cast('string')).writeStream.format("console").trigger(processingTime = '30 second').start()

ds = df.select(col('value').cast('string')) \
.select(from_json('value', schema) \
.alias('json_data')) \
.select('json_data.code') \
.writeStream.format("console") \
.trigger(processingTime = '30 second') \
.start()

+------+
|  code|
+------+
|200000|
|200000|
|200000|
|200000|
|200000|
|200000|
|200000|
|200000|
|200000|
|200000|
|200000|
|200000|
|200000|
|200000|
|200000|
|200000|
+------+


ds = df.select(col('value').cast('string')) \
.select(from_json('value', schema) \
.alias('json_data')) \
.select('json_data.code','json_data.data.time','json_data.data.symbol','json_data.data.buy','json_data.data.sell','json_data.data.changeRate','json_data.data.changePrice','json_data.data.high','json_data.data.low','json_data.data.vol','json_data.data.volValue','json_data.data.last','json_data.data.averagePrice','json_data.data.takerFeeRate','json_data.data.makerFeeRate','json_data.data.takerCoefficient','json_data.data.makerCoefficient') \
.writeStream.format("console") \
.trigger(processingTime = '30 second') \
.start()


+------+-------------+--------+-------+-------+----------+-----------+-------+-------+-------------+-------------------+-------+--------------+------------+------------+----------------+----------------+
|  code|         time|  symbol|    buy|   sell|changeRate|changePrice|   high|    low|          vol|           volValue|   last|  averagePrice|takerFeeRate|makerFeeRate|takerCoefficient|makerCoefficient|
+------+-------------+--------+-------+-------+----------+-----------+-------+-------+-------------+-------------------+-------+--------------+------------+------------+----------------+----------------+
|200000|1646927936005|BTC-USDT|39145.8|39145.9|    -0.077|      -3269|42581.2|38524.5| 9952.7080838|402628810.627642742|39145.9|40645.35223714|       0.001|       0.001|               1|               1|
|200000|1646927942005|BTC-USDT|39142.6|39142.7|     -0.08|    -3408.3|42581.2|38524.5| 9931.0404654|401694971.073026851|39142.6|40645.35223714|       0.001|       0.001|               1|               1|
|200000|1647197110009|BTC-USDT|38959.5|38959.6|   -0.0037|       -145|  39301|38369.5|2988.80991691|116342936.247779102|38959.6|38954.28220059|       0.001|       0.001|               1|               1|
|200000|1647197114006|BTC-USDT|38959.6|38959.7|   -0.0037|     -144.9|  39301|38369.5|2988.87637248|116345525.326978233|38959.7|38954.28220059|       0.001|       0.001|               1|               1|
|200000|1647200622006|BTC-USDT|38914.5|38914.6|   -0.0038|     -151.9|  39301|38369.5| 2943.6804424|114580736.684065049|38914.6|38954.28220059|       0.001|       0.001|               1|               1|
|200000|1647200622006|BTC-USDT|38914.5|38914.6|   -0.0038|     -151.9|  39301|38369.5| 2943.6804424|114580736.684065049|38914.6|38954.28220059|       0.001|       0.001|               1|               1|
|200000|1647203662005|BTC-USDT|38680.9|  38681|   -0.0119|       -469|  39301|38369.5|3046.97270963|118555868.360421858|  38681|38954.28220059|       0.001|       0.001|               1|               1|
|200000|1647203662005|BTC-USDT|38680.9|  38681|   -0.0119|       -469|  39301|38369.5|3046.97270963|118555868.360421858|  38681|38954.28220059|       0.001|       0.001|               1|               1|
|200000|1647265334005|BTC-USDT|38710.5|38710.6|   -0.0026|       -104|39286.3|37555.1|5338.07272971|205818020.571770281|38710.6|38954.28220059|       0.001|       0.001|               1|               1|
|200000|1647265344006|BTC-USDT|38710.8|38710.9|   -0.0026|     -103.7|39286.3|37555.1|5338.18108314|205822215.701085211|38710.9|38954.28220059|       0.001|       0.001|               1|               1|
|200000|1647268210009|BTC-USDT|38946.3|  38951|    0.0015|       62.1|39286.3|37555.1|5483.99120937| 211522452.08024672|38951.6|38954.28220059|       0.001|       0.001|               1|               1|
|200000|1647268214005|BTC-USDT|  38944|38944.1|    0.0014|       54.5|39286.3|37555.1|5483.95856058|211521180.113414936|  38944|38954.28220059|       0.001|       0.001|               1|               1|
|200000|1647355282005|BTC-USDT|39041.6|39041.7|    0.0019|         76|  39912|  38118| 5609.0712851| 218286097.00170912|39041.6|38585.79280689|       0.001|       0.001|               1|               1|
|200000|1647355282005|BTC-USDT|39041.6|39041.7|    0.0019|         76|  39912|  38118| 5609.0712851| 218286097.00170912|39041.6|38585.79280689|       0.001|       0.001|               1|               1|
|200000|1647525958007|BTC-USDT|41129.9|  41130|    0.0171|      693.3|  41500|39302.3|7460.77171397|303533493.831434513|  41125|39233.91485524|       0.001|       0.001|               1|               1|
|200000|1647525964006|BTC-USDT|41129.9|  41130|    0.0157|      639.7|  41500|39302.3|7452.71802948|303207707.085420342|41129.9|39233.91485524|       0.001|       0.001|               1|               1|
+------+-------------+--------+-------+-------+----------+-----------+-------+-------+-------------+-------------------+-------+--------------+------------+------------+----------------+----------------+



dss=df.select(col('value').cast('string')).select(from_json('value',schema).alias('json_data')).select('json_data.data.time','json_data.data.symbol','json_data.data.buy','json_data.data.sell','json_data.data.high','json_data.data.low','json_data.data.changeRate').writeStream.format("parquet").outputMode("append").option("checkpointLocation", "/user/root/checkpointLocation").option("path", "/user/root/path").start()


df1=spark.read.parquet("/user/root/path/part-00000-a00c96f5-9672-4622-bdfe-9871769a7708-c000.snappy.parquet")
df1.show()
+-------------+--------+-------+-------+-------+-------+----------+
|         time|  symbol|    buy|   sell|   high|    low|changeRate|
+-------------+--------+-------+-------+-------+-------+----------+
|1646927936005|BTC-USDT|39145.8|39145.9|42581.2|38524.5|    -0.077|
|1646927942005|BTC-USDT|39142.6|39142.7|42581.2|38524.5|     -0.08|
|1647197110009|BTC-USDT|38959.5|38959.6|  39301|38369.5|   -0.0037|
|1647197114006|BTC-USDT|38959.6|38959.7|  39301|38369.5|   -0.0037|
|1647200622006|BTC-USDT|38914.5|38914.6|  39301|38369.5|   -0.0038|
|1647200622006|BTC-USDT|38914.5|38914.6|  39301|38369.5|   -0.0038|
|1647203662005|BTC-USDT|38680.9|  38681|  39301|38369.5|   -0.0119|
|1647203662005|BTC-USDT|38680.9|  38681|  39301|38369.5|   -0.0119|
|1647265334005|BTC-USDT|38710.5|38710.6|39286.3|37555.1|   -0.0026|
|1647265344006|BTC-USDT|38710.8|38710.9|39286.3|37555.1|   -0.0026|
|1647268210009|BTC-USDT|38946.3|  38951|39286.3|37555.1|    0.0015|
|1647268214005|BTC-USDT|  38944|38944.1|39286.3|37555.1|    0.0014|
|1647355282005|BTC-USDT|39041.6|39041.7|  39912|  38118|    0.0019|
|1647355282005|BTC-USDT|39041.6|39041.7|  39912|  38118|    0.0019|
+-------------+--------+-------+-------+-------+-------+----------+



--============================================---
--step 17: importing etherium file         ===---
--===========================================+---

drag and drop csv file in hdfs
make directory


--============================================---
--step 17: reading etherium file         ===---
--===========================================+---

df2=spark.read.load('/user/root/eth/eth.csv',format='csv',sep='^',inferSchema='true',header='true')
df2.show()
+-------------+-------+------+------+------+------+------------+
|        time1|symbol1|  buy1| sell1| high1|  low1|changeprice1|
+-------------+-------+------+------+------+------+------------+
|1646927936005|    ETH|2797.3|2798.2|2798.3|2796.9|        -0.9|
|1646927942005|    ETH|2796.3|2797.2|2798.1|2797.1|        -0.9|
|1647197110009|    ETH|2799.3|2798.2|2799.5|2797.2|         1.1|
|1647197114006|    ETH|2797.4|2798.5|2798.9|2797.3|        -1.1|
|1647200622006|    ETH|2798.3|2798.9|2799.3|2797.9|        -0.6|
|1647203662005|    ETH|2797.6|2798.6|2798.3|2796.9|        -1.0|
|1647265334005|    ETH|2797.9|2798.9|2799.0|2797.3|        -1.0|
|1647265344006|    ETH|2797.5|2798.7|2798.9|2797.2|        -1.2|
+-------------+-------+------+------+------+------+------------+


df3=df1.alias("btu").join(df2.alias("eth"),col("btu.time") == col("eth.time1"),"inner").select(col("btu.time"),col("btu.symbol"),col("btu.buy"), col("btu.sell"),col("btu.high"),col("btu.low"),col("btu.changeRate"),col("eth.symbol1"),col("eth.buy1"), col("eth.sell1"),col("eth.high1"),col("eth.low1"),col("eth.changeprice1").alias("changeRate1"))


df3.show()
+-------------+--------+-------+-------+-------+-------+----------+-------+------+------+------+------+-----------+
|         time|  symbol|    buy|   sell|   high|    low|changeRate|symbol1|  buy1| sell1| high1|  low1|changeRate1|
+-------------+--------+-------+-------+-------+-------+----------+-------+------+------+------+------+-----------+
|1646927936005|BTC-USDT|39145.8|39145.9|42581.2|38524.5|    -0.077|    ETH|2797.3|2798.2|2798.3|2796.9|       -0.9|
|1646927942005|BTC-USDT|39142.6|39142.7|42581.2|38524.5|     -0.08|    ETH|2796.3|2797.2|2798.1|2797.1|       -0.9|
|1647197110009|BTC-USDT|38959.5|38959.6|  39301|38369.5|   -0.0037|    ETH|2799.3|2798.2|2799.5|2797.2|        1.1|
|1647197114006|BTC-USDT|38959.6|38959.7|  39301|38369.5|   -0.0037|    ETH|2797.4|2798.5|2798.9|2797.3|       -1.1|
|1647200622006|BTC-USDT|38914.5|38914.6|  39301|38369.5|   -0.0038|    ETH|2798.3|2798.9|2799.3|2797.9|       -0.6|
|1647200622006|BTC-USDT|38914.5|38914.6|  39301|38369.5|   -0.0038|    ETH|2798.3|2798.9|2799.3|2797.9|       -0.6|
|1647203662005|BTC-USDT|38680.9|  38681|  39301|38369.5|   -0.0119|    ETH|2797.6|2798.6|2798.3|2796.9|       -1.0|
|1647203662005|BTC-USDT|38680.9|  38681|  39301|38369.5|   -0.0119|    ETH|2797.6|2798.6|2798.3|2796.9|       -1.0|
|1647265334005|BTC-USDT|38710.5|38710.6|39286.3|37555.1|   -0.0026|    ETH|2797.9|2798.9|2799.0|2797.3|       -1.0|
|1647265344006|BTC-USDT|38710.8|38710.9|39286.3|37555.1|   -0.0026|    ETH|2797.5|2798.7|2798.9|2797.2|       -1.2|
+-------------+--------+-------+-------+-------+-------+----------+-------+------+------+------+------+-----------+


df3=df1.alias("btu").join(df2.alias("eth"),col("btu.time") == col("eth.time1"),"inner").select(col("btu.time"),col("btu.symbol"),col("btu.buy"), col("btu.sell"),col("btu.high"),col("btu.low"),col("btu.changeRate"),col("eth.symbol1"),col("eth.buy1"), col("eth.sell1"),col("eth.high1"),col("eth.low1"),col("eth.changeprice1").alias("changeRate1")).where(col("time") == 1647197110009)

df3.show()
+-------------+--------+-------+-------+-----+-------+----------+-------+------+------+------+------+-----------+
|         time|  symbol|    buy|   sell| high|    low|changeRate|symbol1|  buy1| sell1| high1|  low1|changeRate1|
+-------------+--------+-------+-------+-----+-------+----------+-------+------+------+------+------+-----------+
|1647197110009|BTC-USDT|38959.5|38959.6|39301|38369.5|   -0.0037|    ETH|2799.3|2798.2|2799.5|2797.2|        1.1|
+-------------+--------+-------+-------+-----+-------+----------+-------+------+------+------+------+-----------+
