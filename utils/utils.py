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

# Hàm lấy tag theo id
def get_text_by_id(soup, id, tag='div'):
    """Lấy danh sách text trong HTML tags ứng với ID

    Args:
        soup (BeautifulSoup): HTML dưới dịnh dạng BeautifulSoup
        id (str): id của thẻ

    Returns:
        list(str): Danh sách text trong các thẻ
    """
    results = soup.findAll(tag, {'id': id})
    results = [r.text for r in results]
    
    return results
    
# Hàm lấy dữ liệu mỗi giải
def get_prize(soup, p_index, p_number):
    """Lấy dữ liệu của từng dải

    Args:
        p_index (int): Số thứ tự giải (0 là giải đặc biệt, 1 là giải nhất, ...)
        p_number (int): Số lượng giải

    Returns:
        str: Chuỗi gom tất cả giải
    """
    prize_list = []
    for i in range(p_number):
        p = get_text_by_id(soup, 'rs_{}_{}'.format(p_index, i))
        prize_list.append(p)
        
    prize = [p for p in zip(*prize_list)]
    
    return [' '.join([str(p) if p.isnumeric() else 'null' for p in list_p]) for list_p in prize]

# Hàm trích xuất dữ liệu từ html
def extract_data(soup):
    """Lấy dữ liệu từ HTML

    Args:
        soup (BeautifulSoup): HTML dưới dịnh dạng BeautifulSoup

    Returns:
        list(dict): Danh sách dữ liệu trong file HTML đó 
    """
    # Thời gian
    dates = get_text_by_id(soup, 'result_date', 'span')
    dates = [d.split(' ')[-1] for d in dates]
    
    # Giải thưởng
    dacbiet = get_prize(soup, 0, 1)
    giainhat = get_prize(soup, 1, 1)
    giainhi = get_prize(soup, 2, 2)
    giaiba = get_prize(soup, 3, 6)
    giaitu = get_prize(soup, 4, 4)
    giainam = get_prize(soup, 5, 6)
    giaisau = get_prize(soup, 6, 3)
    giaibay = get_prize(soup, 7, 4)
    
    return [{
        'ngay': d,
        'dacbiet': _0,
        'giainhat': _1,
        'giainhi': _2,
        'giaiba': _3,
        'giaitu': _4,
        'giainam': _5,
        'giaisau': _6,
        'giaibay': _7
    } for d, _0, _1, _2, _3, _4, _5, _6, _7 in zip(dates, dacbiet, giainhat, giainhi, giaiba, giaitu, giainam, giaisau, giaibay)]