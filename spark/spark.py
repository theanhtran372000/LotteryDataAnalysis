# --- IMPORT LIBS --- #
from operator import add
from pyspark.sql import SparkSession
import yaml
import time
from datetime import datetime
import os


# --- LOAD SYSTEM CONFIG --- #
with open('config.yaml', 'r') as f:
    config = yaml.load(f, yaml.loader.SafeLoader)
    
    HDFS_HOST = config['hdfs_host']         # HDFS name node
    HDFS_PORT = config['hdfs_port']         # HDFS hdfs port
    HDFS_DIR = config['hdfs_dir']           # HDFS data dir
    HDFS_USER = config['hdfs_user']         # HDFS user
    HDFS_COUNT = config['hdfs_count']       # HDFS data count
    
    SPARK_HOST = config['spark_host']       # Spark host
    SPARK_PORT = config['spark_port']       # Spark port
    SPARK_LOOP = config['spark_loop']       # Spark loop time
    
data_path = '{}/data.csv'.format(HDFS_DIR)  # HDFS data path

print('- ' * 10, 'SYSTEM CONFIG', '- ' * 10)
print('HDFS address: hdfs://{}:{}'.format(HDFS_HOST, HDFS_PORT))
print('HDFS user: {}'.format(HDFS_USER))
print('HDFS data: {}'.format(data_path))
print()


# --- CONNECT TO SPARK --- #
# Thiết lập kết nối tới Spark master (nằm trên HDFS name node)
spark = SparkSession.builder.appName('Lottery Data Analysis').master('spark://hdfs-namenode:7077').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('ERROR')

# In thông tin về spark
print('- ' * 10, '  SPARK  ', '- ' * 10)
print('Spark Context -> Master: {} - App Name: {}'.format(sc.master, sc.appName))


# --- LOOP --- #
while True:
    print('- ' * 20)
    print('Checking dataset at {} ...'.format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
    
    # Đọc file dữ liệu CSV dưới dạng dataframe
    df = spark.read.csv(f'hdfs://{HDFS_HOST}:{HDFS_PORT}{data_path}', inferSchema=False, header=True)
    
    # In sample data
    print('Sample data: ')
    df.show(5, truncate=False)

    # Count
    cnt = df.count()
    print('Number of samples: ', cnt)
    
    # Nếu có thêm dữ liệu mới
    if cnt > HDFS_COUNT:
        start = time.time()
        
        print('There are new data ...')
        
        # Update số liệu mới
        config['hdfs_count'] = cnt
        with open('config.yaml', 'w') as f:
            yaml.dump(config, f)
        
        # Lấy các chữ số cuối
        df_rdd = df.rdd
        prize = df_rdd.map(lambda x: (x['ngay'], x['dacbiet'], x['dacbiet'][-2:], x['dacbiet'][-3:]))
        prize.cache()
        
        # Tạo thư mục kết quả
        os.makedirs('./results', exist_ok=True)
        
        # 100 ngày gần đây nhất
        latest_100 = prize.map(lambda x: (datetime.strptime(x[0], '%d-%m-%Y').strftime('%Y-%m-%d'), x[1])).sortByKey(ascending=False)
        latest_100 = latest_100.toDF(['date', 'number']).limit(100)
        latest_100.toPandas().to_csv('./results/100.csv', index=False)

        # Phân tích dữ liệu đề (2 số cuối)
        _2digits = prize.map(lambda x: (x[2], 1)).reduceByKey(add).sortByKey()
        df_2digits = _2digits.toDF(['number', 'count'])
        df_2digits.toPandas().to_csv('./results/2digit.csv', index=False) # Save dữ liệu đề
        
        # Phân tích dữ liệu 3 càng
        _3digits = prize.map(lambda x: (x[3], 1)).reduceByKey(add).sortByKey()
        df_3digits = _3digits.toDF(['number', 'count'])
        df_3digits.toPandas().to_csv('./results/3digit.csv', index=False) # Save dữ liệu 3 càng
        
        print('Finished updating new data after {:.2f}s!'.format(time.time() - start))
        
    else:
        print('Nothing changed!')
    
    print()
    
    # Sleep 5 phút
    print('Wait {} minutes to check again ...'.format(SPARK_LOOP))
    time.sleep(SPARK_LOOP * 60)