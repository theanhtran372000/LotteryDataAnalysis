# Phân tích dữ liệu lô đề miền Bắc

Bài tập lớn môn Lưu trữ và xử lý dữ liệu lớn do **TS. Nguyễn Hữu Đức** hướng dẫn.

## 1. Phân công công việc
- Trần Thế Anh (20183688): Nhóm trưởng, cài đặt Spark trên Yarn của HDFS, phân tích dữ liệu bằng Spark, làm slide
- Nguyễn Quang Hưng (20183760): Cài đặt Kafka, viết chương trình producer và consumer cho kafka
- Phạm Xuân Lộc (20183786): Tìm nguồn dữ liệu, tìm cách crawl dữ liệu
- Trần Đình Nguyên (20183807): Visualize dữ liệu phân tích được bằng Streamlit 
- Nguyễn Huy Hiếu (20183742): Cài đặt HDFS và kết nối tới Kafka
- Cả nhóm: Lên kế hoạch, đề tài và viết báo cáo

## 2. Mô tả bài tập lớn
### a. Dòng dữ liệu

![System](./imgs/system.png)

**Mô tả:** Nhìn chung, dữ liệu sẽ được crawl từ một trang web về kết quả xổ số miền Bắc [ketqua1.net](https://ketqua1.net/so-ket-qua), sau đó dữ liệu được đẩy vào Kafka để trung chuyển. Tiếp theo, Kafka sẽ lưu dữ liệu gửi tới vào HDFS. Kế tiếp, Spark thông qua YARN để đọc dữ liệu từ HDFS và phân tích dữ liệu, dữ liệu được phân tích xong sẽ được lưu vào hệ thống file local. Cuối cùng, một chương trình được build bằng thư viện Streamlit sẽ đọc dữ liệu đã phân tích và visualize lên.

### b. Đặc điểm nổi bật
- Hadoop và Spark được phân tán trên nhiều máy ảo
- Hệ thống có thể chạy continuous, tức là có dữ liệu mới thì toàn bộ data flow sẽ được update lại

### c. Hình ảnh minh họa
Kết quả cuối cùng được visualize trên Streamlit như sau:

![Streamlit](./imgs/example.png)
