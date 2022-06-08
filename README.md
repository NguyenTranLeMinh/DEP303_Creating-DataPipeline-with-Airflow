***
Thực hành dùng Airflow để tạo một Data Pipeline hoàn chỉnh để có thể làm các thao tác như tải tập dữ liệu, import tập dữ liệu vào Database và xử lý dữ liệu với Spark. Sản phẩm sẽ là một Data Pipeline có các bước như sau:

***

Môi trường:

Sử dụng Ubuntu server cài đặt trên Oracle VM VirtualBox.
Điều khiền từ xa với SSH (trên CMD Prompt hoặc Visual Code).
Cài đặt Airflow Server, Spark và MongoDB trên Ubuntu server đó.

***

Dữ liệu cũng là 2 tệp csv như trong Repo thực hành với PySpark.

1. Questions.csv:

File csv chứa các thông tin liên quan đến câu hỏi của hệ thống, với cấu trúc như sau:

- Id: Id của câu trả lời.

- OwnerUserId: Id của người tạo câu trả lời đó. (Nếu giá trị là NA thì tức là không có giá trị này).

- CreationDate: Ngày câu hỏi được tạo.

- ClosedDate: Ngày câu hỏi kết thúc (Nếu giá trị là NA thì tức là không có giá trị này).

- Score: Điểm số mà người tạo nhận được từ câu hỏi này.

- Title: Tiêu đề của câu hỏi.

- Body: Nội dung câu hỏi.

2. File Answers.csv

File csv chứa các thông tin liên quan đến câu trả lời và có cấu trúc như sau:

- Id: Id của câu hỏi.

- OwnerUserId: Id của người tạo câu hỏi đó. (Nếu giá trị là NA thì tức là không có giá trị này)

- CreationDate: Ngày câu trả lờiđược tạo.

- ParentId: ID của câu hỏi mà có câu trả lời này.

- Score: Điểm số mà người trả lờinhận được từ câu trả lời này.

- Body: Nội dung câu trả lời.

***

1.  Task: start và end

Bạn cần tạo 2 task start và end là các DummyOperator để thể hiện cho việc bắt đầu và kết thúc của DAG.

2. Task: branching

Bạn cần tạo 1 task để kiểm tra xem hai file Questions.csv và Answers.csv đã được tải xuống để sẵn sàng import hay chưa.

Nếu file chưa được tải xuống thì bắt đầu quá trình xử lý dữ liệu (Chuyển đến task clear_file).
Nếu file đã được tải xuống thì sẽ kết thúc Pipleine (Chuyển đến task end).
Để làm được Task này. bạn sẽ cần sử dụng BranchPythonOperator.

3. Task: clear_file

Đây sẽ là Task đầu tiên trong quá trình xử lý dữ liệu,  trước khi Download các file Questions.csv và Answers.csv thì bạn sẽ cần xóa các file đang tồn tại để tránh các lỗi liên quan đến việc ghi đè. Để hoàn thành thao tác này bạn có thể sử dụng BashOperator.

4. Task: dowload_question_file_task và dowload_answer_file_task

Bạn sẽ cần tạo 1 Task để download các file csv cần thiết. Đầu tiên, bạn hãy upload các file csv đó lên Google Drive cá nhân, sau đó sử dụng thư viện google_drive_downloader  và PythonOperator để tải các file đó. Ví dụ như sau:


from google_drive_downloader import GoogleDriveDownloader as gdd

gdd.download_file_from_google_drive(file_id='1iytA1n2z4go3uVCwE__vIKouTKyIDjEq',
                                    dest_path='./data/mnist.zip',
                                    unzip=True)
Đoạn code trên sẽ tải file theo id và lưu xuống đường dẫn dest_path. Để lấy được ID của file trên Google Drive, bạn sẽ cần lấy shareable link của file đó, ví dụ với link như sau:

https://drive.google.com/file/d/1FflYh-YxXDvJ6NyE9GNhnMbB-M87az0Y/view?usp=sharing

Với ví dụ trên, Id của file sẽ là 1FflYh-YxXDvJ6NyE9GNhnMbB-M87az0Y

5. Task: import_questions_mongo và import_answers_mongo

Sau khi tải xuống các file dữ liệu ở dạng csv, bạn sẽ cần Import các dữ liệu đó vào MongoDB để lưu trữ dữ liệu. Bạn có thể sử dụng BashOperator với câu lệnh mongoimport như sau:


mongoimport --type csv -d <database> -c <collection> --headerline --drop <file>

6. Task: spark_process

Bạn cần tạo một Task để có thể sử dụng Spark xử lý các dữ liệu vừa được Import, dữ liệu sẽ được xử lý theo yêu cầu như sau:

Dựa vào tập dữ liệu, hãy tính toán xem mỗi câu hỏi đang có bao nhiêu câu trả lời. Dữ liệu đầu ra sẽ có cấu trúc như sau:


+----+-----------------+                                                        
|  Id|Number of answers|
+----+-----------------+
|  80|                3|
|  90|                3|
| 120|                1|
| 180|                9|
| 260|                9|
| 330|               10|
| 470|                1|
| 580|               14|
| 650|                6|
| 810|                4|
| 930|                7|
|1010|                3|
|1040|                7|
|1070|                4|
|1160|               12|
|1180|                4|
|1300|                7|
|1390|                6|
|1600|                5|
|1610|                8|
+----+-----------------+
only showing top 20 rows

Sau khi tính toán xong, hãy sử dụng DataFrameWriter để lưu dữ liệu đã được xử lý ở dưới dạng .csv

Để hoàn thành task này, bạn có thể sử dụng SparkSubmitOperator để có thể submit một Spark Job vào hệ thống.

7. Task: import_output_mongo

Sau khi đã xử lý xong dữ liệu, bạn hãy lưu các kết quả vào MongoDB dựa vào file .csv đã được export từ Spark. Tương tự với các Task import, bạn có thể sử dụng BashOperator với câu lệnh mongoimport.

8. Sắp xếp thứ tự các Task và thiết lập để thực thi song song:

Bạn sẽ cần sắp xếp lại thứ tự của các Task theo như hình dưới đây:



Đồng thời, cũng có những Task có thể được thực thi song song với nhau để tiết kiệm thời gian hơn:

dowload_question_file_task và dowload_answer_file_task
import_questions_mongo và import_answers_mongo

Bạn hãy thiết lập cho Airflow sử dụng LocalExcutor để có thể thực thi các Task song song.

9. (Nâng cao) Cài đặt CeleryExcutor cho Airflow

Ngoài LocalExcutor thì Airflow cũng hỗ trợ sử dụng CeleryExcutor để thực thi các Task trên các Worker khác nhau. Bạn hãy cài đặt và thiết lập cho Airflow có thể sử dụng CeleryExcutor. 

***

Mã nguồn gồm 2 thư mục:

Thư mục LocalExcutor (chạy Airflow ở chế độ LocalExcutor) với đầy đủ mã nguồn và Grant Chart trên Airflow UI. 

Thư mục Celery_executor chứa tệp airflow.cfg để thiết lập chạy chế độ Celery_executor cùng với hình ảnh chụp Flower UI sau khi chạy mã nguồn y hệt trong LocalExcutor.