# Lấy dữ liệu từ Kafka
# Lưu dữ liệu xuống HDFS

# Import libs
from asyncore import write
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import yaml
import time
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient
import pandas as pd
from utils import *

# Load config file
with open('config.yaml', 'r') as f:
    data = yaml.load(f, Loader=yaml.loader.SafeLoader)
    
    # Lấy dữ liệu config
    # Data source
    URL = data['data_source_url']           # Đường dẫn tới trang web chứa dữ liệu
    COUNT = data['data_source_cnt']         # Số bản ghi đọc trong một lần gửi Request
    CODE = data['data_source_cnt']          # Mã sổ số muốn crawl
    DOW = data['data_source_dow']           # Số ngày trong tuần muốn lấy 1 lúc
    LIMIT = data['data_limit']              # Số năm lấy dữ liệu
    UPDATE_TIME = data['data_update_time']  # Thời gian server update dữ liệu
    
    # Kafka
    TOPIC = data['kafka_topic']             # Topic gửi dữ liệu tới kafka
    KAFKA_SERVER = data['kafka_server']     # Địa chỉ của kafka server
    
    # HDFS
    HDFS_SERVER = data['hdfs_server']       # HDFS name node
    HDFS_DIR = data['hdfs_dir']             # HDFS data dir
    HDFS_USER = data['hdfs_user']           # HDFS user


# Main
if __name__ == '__main__':
    
    # Khởi tạo Consumer subscribe vào Kafka
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset='earliest',
        group_id='consumer_group_a'
    )
    
    # Khởi tạo HDFS client
    # Connect vào HDFS
    print('Connecting to HDFS ...')
    hdfs_client = InsecureClient(HDFS_SERVER, user=HDFS_USER)
    
    # Create data directory
    print('Initializing environment ...')
    if not hdfs_exists(hdfs_client, HDFS_DIR):
        hdfs_client.makedirs('/data')
        
    # Init save file
    path = HDFS_DIR + '/data.csv' # saved path
    if not hdfs_exists(hdfs_client, path):
        df = pd.DataFrame([], columns=['date', 'jackpot'])
        with hdfs_client.write(path, encoding = 'utf-8') as writer:
            df.to_csv(writer, index=False)
    
    print('Start the consumer listening at {} topic {}\n'.format(KAFKA_SERVER, TOPIC))
    for msg in consumer:
        # Convert recieved messages to JSON
        start = time.time()
        msg = msg.value    
        msg = to_json(msg) # json message
        n_samples = len(msg['date']) # số samples
        
        print('-' * 20)
        print('Recieved: {} samples at {}!'.format(n_samples, str(datetime.now())))
        
        # Đọc dữ liệu đang có
        with hdfs_client.read(path) as reader:
            df = pd.read_csv(reader)
            
            # Thêm dữ liệu mới
            df = pd.concat([df, pd.DataFrame(msg, index=range(n_samples))], ignore_index=True)
            
        # Xóa file cũ
        hdfs_client.delete(path)
        
        # Lưu dữ liệu đã update vào HDFS
        with hdfs_client.write(path, encoding = 'utf-8') as writer:
            df.to_csv(writer, index=False)
            
        print('Wrote data to HDFS at {}!'.format(path))
        print('Finished after {:.2f}s!'.format(time.time() - start))
        print()