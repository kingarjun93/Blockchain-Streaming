#Importing Modules
from kafka import KafkaConsumer
import os
import json
import datetime

#Subscribing Kafka Consumer to the topic
consumer = KafkaConsumer('BlockchainTopic')
test=''
i=0

#Store the message for every msg recieved in consumer demonstrating Producer - Consumer communication
for msg in consumer:
    now = datetime.datetime.now()
    folder_path = 'D:\MAD tasks\Transactions_consumer\yearno='+str(now.year)+'\monthno='+str(now.month)+'\dayno='+str(now.day)+'\hourno='+str(now.hour)+'\minno='+ str(now.minute)+'\\'
    print('D:\MAD tasks\Transactions_consumer\yearno='+str(now.year)+'\monthno='+str(now.month)+'\dayno='+str(now.day)+'\hourno='+str(now.hour)+'\minno='+ str(now.minute)+'\\')
    file_path = folder_path+ str(now.second)+'_response.txt'
    if(not (os.path.isdir(folder_path) and os.path.exists(folder_path))):
        os.makedirs(folder_path)
    text_file = open(file_path, "w")
    result=json.loads(msg.value.decode())['x']['inputs']
    text_file.write(str(result))
    text_file.close()
    
    print("message ",i," recieved...")
    i=i+1
    
    
