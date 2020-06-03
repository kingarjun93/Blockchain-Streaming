#Importing Modules
from flask import Flask,jsonify,request
from werkzeug.wrappers import Request, Response
import redis
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
sc = SparkContext('local')
spark = SparkSession(sc)

app = Flask(__name__)

#Folder contaning data
file_path='D:\MAD tasks\Transactions\yearno=*\monthno=*\dayno=*\hourno=*\minno=*\*.txt'

#Establish connection to redis server
r = redis.Redis(
    host='localhost',
    port='6379', 
    password='')

#To extract dataframe show string
def getShowString(df, n=20, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return(df._jdf.showString(n, 20, vertical))
    else:
        return(df._jdf.showString(n, int(truncate), vertical))

#Definitions for endpoints
@app.route('/show_transactions', methods=['GET'])
def show_transactions_func():
    # Redis code to get last 100 transactions
    print("===================================================")
    print("Request recieved for show_transactions")
    print("===================================================")
    total_results = []
    count=0
    cur, results = r.scan(0,'transactions*',1000)
    total_results += results

    while cur != 0:
        cur, results = r.scan(cur,'transactions*',1000)
        total_results += results

    for key in total_results:
        #print(type(key))
        val=r.get(key.decode("utf-8"))
        count+=int(val)

    print("===================================================")
    return ({"count": str(count)})
    #return ({"about": "Hello World!"})

@app.route('/transactions_count_per_minute/<int:min_value>', methods=['GET'])
def transactions_count_per_minute(min_value):
    # Redis code to display transactions per min
    print("===================================================")
    print("Request recieved for transactions_count_per_minute")
    print("===================================================")
    df=spark.read.json(file_path)
    sqlc = SQLContext(sc)
    df.registerTempTable('test')
    res=sqlc.sql("WITH inputs AS ( \
    SELECT to_timestamp(x.time) AS time_of_transaction,x.hash AS hash, explode(x.inputs) AS in FROM test), \
    trans AS(SELECT in.prev_out.addr AS Address, \
    in.prev_out.value AS SatoshiAmount, \
    time_of_transaction, \
    minute(time_of_transaction)||':'||second(time_of_transaction) AS minute, \
    hash \
    FROM inputs \
    WHERE time_of_transaction >= (current_timestamp - INTERVAL 1 HOUR) \
    ) \
    SELECT minute,COUNT(*) AS counts_morethan_" +str(min_value)+" \
    FROM trans \
    GROUP BY minute \
    HAVING COUNT(*) > " + str(min_value) +" \
    ORDER BY minute")
    
    print(str(min_value))
    print(getShowString(res))
    #return 'To display ' + str(min_value) + ' transactions per minute'
    print("===================================================")
    return getShowString(res)

@app.route('/high_value_addr', methods=['GET'])
def high_value_addr():
    print("===================================================")
    print("Request recieved for high_value_addr")
    print("===================================================")
    df=spark.read.json(file_path)
    sqlc = SQLContext(sc)
    df.registerTempTable('test')
    res=sqlc.sql("WITH inputs AS ( \
    SELECT to_timestamp(x.time) AS time_of_transaction,x.hash AS hash, explode(x.inputs) AS in FROM test), \
    trans AS \
    (SELECT in.prev_out.addr AS Address, \
    in.prev_out.value AS SatoshiAmount, \
    time_of_transaction, \
    hash \
    FROM inputs \
    WHERE time_of_transaction >= (current_timestamp - INTERVAL 3 HOUR) \
    ) \
    SELECT address,SUM(SatoshiAmount) AS total_value \
    FROM trans \
    GROUP BY address \
    ORDER BY 2 DESC")

    #return "code to display aggregrate sum of high value addr"
    print(getShowString(res))
    print("===================================================")
    return (getShowString(res))

@app.route("/")
def hello():
    return ({"about": "Hello World!"})

if __name__ == '__main__':
    from werkzeug.serving import run_simple
    #app.run(debug=True)
    run_simple('localhost', 9000, app)
