# Lấy dữ liệu từ Kafka
# Lưu dữ liệu xuống HDFS

# Import libs
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import yaml
import time
from kafka import KafkaConsumer
from hdfs import InsecureClient
import pandas as pd
from utils import *

# Load config file
with open('config.yaml', 'r') as f:
    config = yaml.load(f, Loader=yaml.loader.SafeLoader)
    
    # Lấy dữ liệu config
    # Kafka
    TOPIC = config['kafka_topic']             # Topic gửi dữ liệu tới kafka
    KAFKA_SERVER = config['kafka_server']     # Địa chỉ của kafka server
    
    # HDFS
    HDFS_HOST = config['hdfs_host']       # HDFS name node
    HTTP_PORT = config['http_port']           # HDFS http port
    HDFS_DIR = config['hdfs_dir']             # HDFS data dir
    HDFS_USER = config['hdfs_user']           # HDFS user


# Main
if __name__ == '__main__':
    
    # Khởi tạo Consumer subscribe vào Kafka
    print('Connecting to Kafka ...')
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset='earliest',
        group_id='consumer_group_a'
    )
    
    # Khởi tạo HDFS client
    # Connect vào HDFS
    print('Connecting to HDFS ...')
    hdfs_client = InsecureClient(f'http://{HDFS_HOST}:{HTTP_PORT}', user=HDFS_USER) # Connect qua HTTP
        
    # Init save path
    path = HDFS_DIR + '/data.csv' # saved path
    # if not hdfs_exists(hdfs_client, path):
    #     df = pd.DataFrame([], columns=['ngay', 'dacbiet'])
    #     with hdfs_client.write(path, encoding = 'utf-8') as writer:
    #         df.to_csv(writer, index=False)
    
    print('Start the consumer listening at {} topic {}\n'.format(KAFKA_SERVER, TOPIC))
    for msg in consumer:
        # Convert recieved messages to JSON
        start = time.time()
        msg = msg.value    
        msg = to_json(msg) # json message
        d = msg['ngay']
        
        print('-' * 20)
        print('Recieved: data at {}!'.format(d))
        print('Data: ', msg)
        
        # Dữ liệu mới
        df = pd.DataFrame(msg, index=[0])
        
        # Append dữ liệu mới vào file csv nếu đã tồn tại
        if hdfs_exists(hdfs_client, path):
            with hdfs_client.write(path, encoding = 'utf-8', append=True) as writer:
                df.to_csv(writer, index=False, header=False)
        
        # Ngược lại thì tạo mới
        else:
            with hdfs_client.write(path, encoding = 'utf-8') as writer:
                df.to_csv(writer, index=False)
            
        print('Wrote data to HDFS at {}!'.format(path))
        print('Finished after {:.2f}s!'.format(time.time() - start))
        print()