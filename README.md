# End-to-End Retail ETL Pipeline

Technology used: *Python, Apache Airflow, Docker, DBT, Google Cloud Storage, Google Cloud Bigquery, Power BI*

![Data Pipeline Diagram](https://github.com/saksit63/retail-project/blob/main/img/retail_workflow.png)

## Process
1. นำข้อมูลการแสดงความคิดของลูกค้าจากไฟล์ CSV และข้อมูลการซื้อขายจากฐานข้อมูล MySQL ไปยัง Google Cloud Storage (GCS)
2. นำข้อมูลการแสดงความคิดเห็นจากลูกค้าไปประมวลผลว่า ความคิดเห็นนั้นไปในทิศทางที่ดีหรือไม่ โดยใช้ PySpark ที่รันบนคลัสเตอร์ของ Google Dataproc
3. นำข้อมูลการซื้อขายและข้อมูลการแสดงความคิดเห็นจากลูกค้าเข้าไปยัง Google Bigquery
4. สร้าง Dashbord โดยใช้ Looker Studio เพื่อช่วยให้ทีมการตลาดและพัฒนาผลิตภัณฑ์เข้าใจความคิดเห็นของลูกค้าและพฤติกรรมการซื้อได้ดีขึ้น นำไปสู่การตัดสินใจทางธุรกิจที่มีประสิทธิภาพมากขึ้นและการปรับปรุงผลิตภัณฑ์ให้ตรงกับความต้องการของลูกค้า
5. กระบวนการทั้งหมดถูกจัดการด้วย Apache Airflow และ Docker

## Source code
DAG file: [retail.py](https://github.com/saksit63/retail-project/blob/main/dags/retail_project.py)

dbt models: [models](https://github.com/saksit63/retail-project/tree/main/dbt_project/models)

dbt tests: [tests](https://github.com/saksit63/retail-project/tree/main/dbt_project/tests)


## Result
Airflow:

![Airflow](https://github.com/saksit63/retail-project/blob/main/result/Airflow_Task_Run.png)

Bigquery:

![Bigquery](https://github.com/saksit63/retail-project/blob/main/result/bigquery.png)

Dashboard: 

![Dashboard](https://github.com/saksit63/retail-project/blob/main/result/retail_dash)

