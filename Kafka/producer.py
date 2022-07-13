# Kết nối và lấy dữ liệu Sổ số miền Bắt từ trang web: https://ketqua1.net/so-ket-qua
# Sau đó gửi dữ liệu tới Kafka

# Import libs
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import requests
import bs4
import yaml
import time
import schedule
from datetime import datetime, timedelta
from kafka import KafkaProducer
from utils import *


# Load config file
with open('config.yaml', 'r') as f:
    config = yaml.load(f, Loader=yaml.loader.SafeLoader)
    
    # Lấy dữ liệu config
    # Data source
    URL = config['data_source_url']           # Đường dẫn tới trang web chứa dữ liệu
    COUNT = config['data_source_cnt']         # Số bản ghi đọc trong một lần gửi Request
    CODE = config['data_source_cnt']          # Mã sổ số muốn crawl
    DOW = config['data_source_dow']           # Số ngày trong tuần muốn lấy 1 lúc
    LIMIT = config['data_limit']              # Số năm lấy dữ liệu
    UPDATE_TIME = config['data_update_time']  # Thời gian server update dữ liệu
    
    # Kafka
    TOPIC = config['kafka_topic']             # Topic gửi dữ liệu tới kafka
    KAFKA_SERVER = config['kafka_server']           # Địa chỉ của kafka server

# Lấy 1 mẫu dữ liệu mới mỗi ngày
def get_new_data(producer):
    now = datetime.now()
    payload = {
        'code': CODE,
        'date': to_string(now),
        'count': 1,
        'dow': DOW
    }
    
    response = requests.post(URL, data=payload, headers={'User-Agent': 'Mozilla/5.0'})
    
    # Parse dữ liệu vào BeautifulSoup
    soup = bs4.BeautifulSoup(response.text, 'html.parser')
    
    # Extract data
    data = extract_data(soup)[0]
    
    if data['ngay'] == now.strftime('%d:%m:%Y'):
        producer.send(TOPIC, data)
        print('Read and sent new data at {}!'.format(str(now)))
        print('Data:', data)
    else:
        print('There were no new data at {}!'.format(str(now)))

# Main
if __name__ == '__main__':
    
    # Init variables
    now = datetime.now()
    
    # Chưa đến giờ ra số đề
    if now.strftime('%H:%M:$S') < UPDATE_TIME:
        now = now - timedelta(days=1)
    
    limit = now - timedelta(days = LIMIT * 365) # Số năm đọc dữ liệu
    
    # Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=json_serializer
    )
    
    # # Config scheduler
    schedule.every().day.at(UPDATE_TIME).do(lambda: get_new_data(producer))
    
    while now > limit:
        # Gửi post request để lấy dữ liệu HTML
        start = time.time()
        
        print('-' * 20)
        print('Reading from {} to {} ...'.format(to_string(now), to_string(now - timedelta(days=COUNT - 1))))
        payload = {
            'code': CODE,
            'date': to_string(now),
            'count': COUNT,
            'dow': DOW
        }
        response = requests.post(URL, data=payload, headers={'User-Agent': 'Mozilla/5.0'})
        
        # Parse dữ liệu vào BeautifulSoup
        soup = bs4.BeautifulSoup(response.text, 'html.parser')
        
        # Extract data
        data_list = extract_data(soup)
        print('Read {} samples!'.format(len(data_list)))
        
        # Gửi dữ liệu tới Kafka Server
        print('Sending data to server {} - topic {} ...'.format(KAFKA_SERVER, TOPIC))
        
        # Nếu có dữ liệu thì gửi
        if len(data_list) > 0:
            for d in data_list: # Gửi từng gói 1
                producer.send(TOPIC, d)
        
        print('Finished after {:.2f}s!'.format(time.time() - start))
        print()
        
        # Giảm thời gian crawl xuống
        now = now - timedelta(days=COUNT)

    print('Done!')
    print()

    # Wait for new data
    print('-' * 20)
    print('Start waiting new data ...')
    
    while True:
        # Check for schedule
        schedule.run_pending()
        time.sleep(1)
    