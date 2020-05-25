import os
from kafka import KafkaProducer
from time import sleep
from websocket import create_connection
import datetime

blockchain_api_url='wss://ws.blockchain.info/inv'
ws = create_connection(blockchain_api_url)

print("Subscribing 'unconfirmed_sub'")
ws.send("{\"op\":\"unconfirmed_sub\"}")

producer = KafkaProducer(bootstrap_servers='localhost:9092')
fullresult = []
i=0
while True:
    result =  ws.recv()
    
    now = datetime.datetime.now()
    folder_path = 'D:\MAD tasks\Transactions\yearno='+str(now.year)+'\monthno='+str(now.month)+'\dayno='+str(now.day)+'\hourno='+str(now.hour)+'\minno='+ str(now.minute)+'\\'
    print('D:\MAD tasks\Transactions\yearno='+str(now.year)+'\monthno='+str(now.month)+'\dayno='+str(now.day)+'\hourno='+str(now.hour)+'\minno='+ str(now.minute)+'\\')
    file_path = folder_path+ str(now.second)+'_response.txt'
    if(not (os.path.isdir(folder_path) and os.path.exists(folder_path))):
        os.makedirs(folder_path)
    text_file = open(file_path, "w")
    text_file.write(result)
    text_file.close()
    
    print("message ",i," sent...")
    i=i+1
    producer.send('BlockchainTopic', bytes(result.encode('utf_8')))
    sleep(20)
