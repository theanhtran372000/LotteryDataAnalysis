import yaml
from hdfs import InsecureClient
import pandas as pd
from tabulate import tabulate

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
    
print('Connecting to HDFS ...')
hdfs_client = InsecureClient(f'http://{HDFS_HOST}:{HTTP_PORT}', user=HDFS_USER) # Connect qua HTTP

with hdfs_client.read('/data/data.csv') as reader:
    df = pd.read_csv(reader)
    print('Number of samples: ', len(df))
    print(tabulate(df.head(10), headers='keys', tablefmt='psql'))