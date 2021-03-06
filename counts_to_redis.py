#Importing Modules
import datetime
import redis
from time import sleep
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext('local')
spark = SparkSession(sc)

#Establish connection to redis server
r = redis.Redis(
    host='localhost',
    port='6379', 
    password='')

#For every 30 seconds. Store count to redis with expiry of 1 hour
while True:
    now = datetime.datetime.now() - datetime.timedelta(minutes=1)

    analysed_df=spark.read.json('D:\MAD tasks\Transactions\yearno='+str(now.year)+'\monthno='+str(now.month)+'\dayno='+str(now.day)+'\hourno='+str(now.hour)+'\minno='+ str(now.minute)+'\*.txt')
    analysed_df.registerTempTable('test')
    sqlc = SQLContext(sc)
    analysed_df=sqlc.sql(" \
    WITH inputs AS ( \
    SELECT to_timestamp(x.time) AS time_of_transaction \
        , explode(x.inputs) AS in \
        FROM test) \
    SELECT 1 FROM inputs \
    ")

    r.set('transactions:'+str(now.hour)+':'+str(now.minute), analysed_df.count(),ex=3600)
    sleep(30)



