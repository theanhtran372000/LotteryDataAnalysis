import json

# Convert data từ dạng serial sang json
def to_json(data):
    """Convert string to JSON object

    Args:
        data (str): String data

    Returns:
        json: Json object
    """
    return json.loads(data)

# Hàm chuyển dữ liệu sang dạng serial (chuỗi) cho producer
def json_serializer(data):
    """Convert json object to string

    Args:
        data (json): Json object

    Returns:
        str: Json string
    """
    return json.dumps(data).encode('utf-8')

# Chuyển thời gian thành dạng string phù hợp với format của request
def to_string(date):
    """Convert datetime to string format dd-mm-YYYY

    Args:
        date (datetime): datetime object

    Returns:
        str: string format dd-mm-YYYY
    """
    return date.strftime("%d-%m-%Y")

# Hàm kiểm tra đường dẫn tồn tại trong HDFS
def hdfs_exists(client, path):
    """Check if a path exists or not

    Args:
        client (InsecureClient): HDFS Client
        path (str): Path to file/directory

    Returns:
        bool: return True if the path exists and vice versa
    """
    return client.status(path, strict=False) is not None