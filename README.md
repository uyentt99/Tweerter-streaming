# TweerterStreming
STREAMING TWITTER & PHÂN TÍCH XU HƯỚNG SỬ DỤNG ĐIỆN THOẠI CỦA NGƯỜI DÙNG TWITTER
# Thành viên nhóm:
- Phạm Minh Khiêm 
- Nguyễn Văn Chức
- Trần Thị Uyên 
- Ngô Văn Giang 
# Mục đích :
- Nhóm mong muốn xây dựng mô phỏng một hệ thống big data, mục đích là streaming twitter tweets của người dùng twitter từ hệ thống TWITTER API, tiền xử lý dữ liệu, lưu vào hệ thống và thực hiện các tính toán phân tích về xu hướng sử dụng điện thoại của người dùng TWITTER
- Các thông tin phân tích trả lời các câu hỏi :
    + Người dùng TWITTER sử dụng thiết bị nào để truy cập ?
    + Người dùng TWITTER sử dụng các thiết bị IOS hay sử dụng các thiết bị ANDROID ? 
    + Quốc gia nào có nhiều người dùng sử dụng thiết bị IOS nhất dựa trên lịch sử tweet ?
    + Quốc gia nào có nhiều người dùng thích sử dụng thiết bị ANDROID nhất ?
    + ..
   
# Thành phần của hệ thống:    
- Hệ thống mô phỏng bao gồm các thành phần như một hệ thống thật, cài đặt trên máy ảo Docker
- Các thành phần trong hệ thống :
    + 1 Cụm kafka dùng để streaming dữ liệu
    + 1 Cụm spark dùng để xử lý streaming và phân tích 
    + 1 Cụm hadoop dùng để lưu trữ dữ liệu phục vụ mục đích phân tích
    + 1 Application nhận đầu vào là TWITTER API, đẩy dữ liệu vào kafka
    + 1 Pyspark notebook để phân tích, visualize
![system](https://github.com/uyentt99/TweerterStreming/system.png)

 # File thư mục:
 - Report_document: file PDF báo cáo
 - notebook: file jupyter notebook báo cáo, bao gồm code để xử lý và biểu diễn dữ liệu sau khi đã steaming vê cụm HDFS
 - 1 số docker-compose cho HDFS, Spark và pyspark
 - bigdata- Source code java để nhận dữ liệu từ twitterAPI, bắn vào kafka của
hệ thống.
 
