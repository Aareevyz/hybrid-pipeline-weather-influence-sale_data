# Weather & Sales Data Pipeline (Hybrid Cloud)

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Glue%20%7C%20SQS%20%7C%20Step%20Functions-orange)
![Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C)
![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-336791)

A robust **Hybrid Data Engineering Pipeline** that bridges the gap between on-premise data sources and cloud-based processing power. This project demonstrates how to ingest local CSV data, process it using **AWS Serverless** technologies, and sync the enriched results back to a local PostgreSQL database using an event-driven architecture.

---

## 🏗️ Architecture

The pipeline follows a **Pull-based** architecture to solve hybrid connectivity challenges:

<img width="100%" alt="Pipeline Architecture" src="https://github.com/user-attachments/assets/f404e466-e454-450d-a817-d9150c072fbc" />

### Key Considerations
*   **Idempotency**: Ingestion script uses MD5 checksums to prevent duplicate uploads.
*   **Schema Evolution**: AWS Glue handles schema checks, and the local exporter dynamically adapts to new columns.
*   **Security**: Uses SQS for signaling, avoiding the need to open local firewall ports for inbound traffic.

---

## 🛠️ Tech Stack

*   **Language**: Python 3.10+
*   **Cloud Provider**: AWS
    *   **S3**: Data Lake storage (Raw/Silver/Gold layers).
    *   **AWS Glue**: Serverless Spark for data transformation and cleaning.
    *   **Amazon SQS**: Message queue for decoupling cloud and local processes.
    *   **Step Functions**: Workflow orchestration.
*   **Database**: PostgreSQL
*   **Libraries**: `boto3`, `pandas`, `pyspark`, `sqlalchemy`, `python-dotenv`, `psycopg2-binary`.

---

## 📂 Project Structure

```bash
📦 Weather & Sales Data Pipeline
 ┣ 📂 0.data_src             # Mock data generation scripts
 ┣ 📂 1.ingestion            # Scripts for uploading local data to S3
 ┣ 📂 2.transformation       # AWS Glue scripts (Silver/Gold layers)
 ┣ 📂 3.export_to_postgres   # Worker scripts for syncing Cloud -> Local DB
 ┣ 📜 Orchestration_StepFunction.json  # AWS Step Functions Definition
 ┗ 📜 Pipfile                # Dependency management
```

---

## 🚀 Setup & Configuration

### 1. Prerequisites
*   Python 3.10+ installed.
*   AWS CLI configured with appropriate permissions (`s3`, `glue`, `sqs`).
*   PostgreSQL installed locally.

### 2. IAM Role & Policy Configuration
To ensure smooth operation, configure the following IAM permissions:

#### A. Local User (Ingestion & Worker)
Attach these policies to the IAM User used by your local scripts (via `aws configure`):
*   **S3 Access**: Read/Write access to the `weather-influence-on-sales` bucket.
*   **SQS Access**: `sqs:ReceiveMessage`, `sqs:DeleteMessage` for the worker queue.

#### B. AWS Glue Service Role
Role: `AWSGlueServiceRole`
*   **Managed Policy**: `AWSGlueServiceRole`, `AWSAthenaFullAccess`
*   **Custom Policy (S3 Access)**:
    ```json
    {
        "Effect": "Allow",
        "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
        "Resource": ["arn:aws:s3:::weather-influence-on-sales", "arn:aws:s3:::weather-influence-on-sales/*"]
    }
    ```
*   **Custom Policy (SQS Access)**:
    ```json
    {
        "Effect": "Allow",
        "Action": ["sqs:SendMessage"],
        "Resource": "arn:aws:sqs:ap-northeast-1:1234567890:my-queue"
    }
    ```

#### C. Lambda Functions
*   **Lambda1 (Ingest)**: Needs `s3:GetObject`.
*   **Lambda2 (Process)**: Needs `s3:PutObject`.
*   **Lambda3 (Trigger)**: Needs `sqs:SendMessage`, Need to Add 'requests' library in lambda layer.

### 3. Step Functions Role
Role: `StepFunctionsExecutionRole`
*   **Policies**: Allow execution of Glue Jobs and Lambda Invoke.

## 🏃‍♂️ Installation & Usage

### 1. Environment Setup
Create a `.env` file in the `3.export_to_postgres` directory:
```ini
SQS_QUEUE_URL=https://sqs.ap-northeast-1.amazonaws.com/1234567890/my-queue
DB_HOST=localhost
DB_USER=your_user
DB_PASSWORD=your_password
```

### 2. Install Dependencies
```bash
pip install boto3 pandas sqlalchemy psycopg2-binary python-dotenv
# OR using Pipenv
pipenv install
```

## 🏃‍♂️ Usage

1.  **Generate Data**: Run `data_set_mockup.py` to create fresh sales data.
2.  **Ingest Data**: Run `raw_data_from_local.py` to upload files to S3.
    ```bash
    python 1.ingestion/raw_data_from_local.py
    ```
3.  **Run ETL (AWS)**: Trigger the AWS Glue Job (or Step Function) manually or via scheduler.
4.  **Sync to Local DB**: Run `sqs_worker.py` to pull results from SQS and sync to local DB.
    ```bash
    python 3.export_to_postgres/sqs_worker.py
    ```

---

## 🔮 Future Improvements
*   **Containerization**: Dockerize the local worker for easier deployment.
*   **Airflow**: Replace custom scripts with Apache Airflow for DAG management.
*   **Data Quality**: Integrate **Great Expectations** for rigorous testing.
