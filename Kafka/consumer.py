# Lấy dữ liệu từ Kafka
# Lưu dữ liệu xuống HDFS

# Import libs
from kafka import KafkaConsumer
import json
import yaml

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
    
    # Kafka topic
    TOPIC = data['kafka_topic']             # Topic gửi dữ liệu tới kafka
    SERVER = data['kafka_server']           # Địa chỉ của kafka server


# Convert data từ dạng serial sang json
def to_json(data):
    return json.loads(msg.value)


# Main
if __name__ == '__main__':
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=SERVER,
        auto_offset_reset='earliest',
        group_id='consumer_group_a'
    )
    
    print('Start the consumer ...')
    for msg in consumer:
        print('Recieved: {}'.format(to_json(msg)))