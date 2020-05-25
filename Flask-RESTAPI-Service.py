from flask import Flask,jsonify,request
from werkzeug.wrappers import Request, Response

app = Flask(__name__)

file_path='D:\MAD tasks\Transactions\yearno=*\monthno=*\dayno=*\hourno=*\minno=*\*.txt'



@app.route('/show_transactions', methods=['GET'])
def show_transactions_func():
    # Redis code to get last 100 transactions
    
    return 'To display last 100 transactions'

@app.route('/transactions_count_per_minute/<int:min_value>', methods=['GET'])
def transactions_count_per_minute(min_value):
    # Redis code to display transactions per min
    return 'To display ' + str(min_value) + ' transactions per minute'

@app.route('/high_value_addr', methods=['GET'])
def high_value_addr():
    
    df=spark.read.json(file_path)
    res=df.selectExpr("to_timestamp(x.time) AS time_of_transaction"," explode(x.inputs) AS in") \
    .selectExpr("in.prev_out.addr AS Address","in.prev_out.value AS SatoshiAmount","time_of_transaction") \
    .filter("time_of_transaction >= (current_timestamp - INTERVAL 3 HOUR)") \
    .selectExpr("address","int(SatoshiAmount) AS SatoshiAmount")
    return "code to display aggregrate sum of high value addr"

@app.route("/")
def hello():
    return ({"about": "Hello World!"})

if __name__ == '__main__':
    from werkzeug.serving import run_simple
    #app.run(debug=True)
    run_simple('localhost', 9000, app)
