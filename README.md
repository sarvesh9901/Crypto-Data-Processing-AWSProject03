# 🚀 Crypto Data Processing Project

This project is designed to continuously process and store crypto transaction data using AWS services. The entire pipeline is serverless, scalable, and suitable for real-time data streaming and processing.

---

## 📌 Overview

Our application generates **crypto transaction data continuously**, which we store and process using various AWS services. The pipeline includes:

- DynamoDB for storing incoming transactions
- Kinesis Data Stream & Firehose for real-time data capture and processing
- S3 Buckets for storing processed data and metadata
- AWS Lambda for lightweight transformation
- AWS Glue (Crawler + ETL) for data processing and storing as Hudi table
- Amazon Athena for querying the processed data
- (Optional) AWS QuickSight for data visualization and dashboarding

---

## 🔄 Data Flow Architecture

1. **Data Generation**
   - Crypto transactions are generated continuously by an application.
   - These records are inserted into a DynamoDB table (`CryptoTransactions`).

2. **Capture DynamoDB Streams (CDC)**
   - DynamoDB Streams are enabled on the table to capture change data (CDC).
   - These changes are forwarded to an **Amazon Kinesis Data Stream**.

3. **Kinesis → Firehose → S3**
   - Kinesis Data Stream is connected to an **Amazon Kinesis Data Firehose**.
   - A Lambda function is attached to Firehose to perform basic **transformation** on the data.
   - The transformed data is stored in the first **S3 bucket**.

4. **AWS Glue Processing**
   - A **Glue Crawler** scans the S3 bucket and creates a table in the Glue Data Catalog.
   - We use a **JSON classifier** with the crawler to understand the structure of the data.
   - A **Glue ETL Job** (written in PySpark) is created to process this data.
   - The ETL job applies transformations and stores the output as a **Hudi table** in the second S3 bucket (used for metadata and optimized querying).

5. **Query with Athena**
   - The processed Hudi data is queried using **Amazon Athena**.

---

## 🧪 Technologies Used

- **AWS DynamoDB** – Stores incoming transactions
- **AWS Kinesis Data Stream** – Captures real-time change events from DynamoDB
- **AWS Kinesis Data Firehose** – Delivers real-time data to S3
- **AWS Lambda** – Applies transformation on streaming data
- **Amazon S3** – Stores raw and processed data
- **AWS Glue (Crawler & ETL)** – Processes data and stores it as Hudi
- **Amazon Athena** – Queries the Hudi data directly from S3
- **(Future)** AWS QuickSight – To build dashboards and visualize the data

---

## ⚙️ Setup Checklist

- [x] DynamoDB table created and Streams enabled
- [x] Kinesis Data Stream created and attached to DynamoDB Stream
- [x] Kinesis Firehose created and attached to Kinesis Stream
- [x] Lambda function created and linked to Firehose for transformation
- [x] S3 Buckets created: one for processed data, another for Hudi/metadata
- [x] Glue Database and Crawler created with JSON classifier
- [x] Glue ETL job created to transform data and save as Hudi
- [x] Athena configured to query Hudi table

---

## 🔮 Future Scope

- 📊 **Dashboarding with QuickSight** – We plan to visualize processed crypto data through interactive dashboards.
- 📁 **Data Partitioning** – Improve query performance by partitioning Hudi data based on date or trading pair.
- 📉 **Analytics & ML** – Use the dataset for analytics or machine learning use cases.

---

## 📂 Folder Structure Suggestion

```plaintext
.
├── lambda/                      # Lambda function code
├── glue-jobs/                  # Glue ETL PySpark scripts
├── athena-queries/             # Saved Athena queries
├── docs/                       # Architecture diagrams or notes
├── README.md                   # This file
