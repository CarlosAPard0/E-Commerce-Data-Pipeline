# 🛒 E-Commerce Data Pipeline: GCS to BigQuery via Dataproc (PySpark)

## 📌 Overview
This project implements a scalable **ETL (Extract, Transform, Load)** pipeline for e-commerce data. It orchestrates the flow of information from a Data Lake (Google Cloud Storage) to a Data Warehouse (BigQuery), using **Apache Airflow** as the orchestrator and **Dataproc Serverless (PySpark)** for heavy data processing.

The unique value of this implementation is the **local hybrid setup**, where Airflow runs via **Astro CLI** while interacting seamlessly with Google Cloud infrastructure.

---

## 🎯 Project Objective
The goal is to transform raw JSON data (Orders and Products) into an **enriched analytical dataset**. This enables business users to:
* **Identify high-value orders** through automated price tiering.
* **Monitor stock levels** in real-time.
* **Calculate total revenue** per transaction without manual calculations.

---

## 🏗️ Architecture
The pipeline follows a modern data stack architecture:

![Project Architecture](./Images/Diagram.png)

1.  **Orchestration:** Airflow triggers ingestion and processing tasks.
2.  **Ingestion:** Raw JSON files are loaded from **GCS** into **BigQuery** staging tables.
3.  **Processing:** A **PySpark** job is submitted to **Dataproc Serverless** to perform complex joins and business logic.
4.  **Storage:** The final, cleaned, and enriched data is stored in **BigQuery**.

---

## 📂 Repository Structure & Components

* **`/datasets`**: Contains the raw data used for this project.
    * `products/products.json`: Catalog data (ID, name, category, price, stock).
    * `orders/orders.json`: Transactional data (Order ID, User ID, Product ID, Date).
* **`/scripts`**: Core transformation logic.
    * `spark_retail_transformation.py`: The PySpark script executed by Dataproc to join tables and apply business logic.
* **`setup_retail_schema.sql`**: Contains the DDL (Data Definition Language) used in BigQuery to initialize the dataset and table schemas.
* **`dags/`**: The Airflow DAG definition script.
* **`.env`**: Configuration for `GOOGLE_APPLICATION_CREDENTIALS` and local environment variables.

---

## 🛠️ Tech Stack & Resources
* **Orchestrator:** Apache Airflow (via Astro CLI).
* **Compute:** Google Dataproc Serverless (Spark 3.x).
* **Storage:** Google Cloud Storage (GCS) & BigQuery.
* **Languages:** Python, PySpark, SQL.
* **Infrastructure:** Docker (Managed by Astro CLI).
* **Security:** IAM Service Accounts with granular roles (BigQuery Admin, Dataproc Worker, Storage Object Admin).

---

## 🚀 Key Features & Implementation
* **Hybrid Orchestration:** Optimized `docker-compose.yaml` and Astro settings to allow a local container to authenticate with GCP.
* **Serverless Transformation:** Used Dataproc Serverless to run Spark jobs, avoiding the cost of maintaining a 24/7 cluster.
* **Dynamic Batching:** Generated unique `batch_id` using `uuid` for every Dataproc run to prevent naming collisions.
* **Advanced IAM Management:** Hands-on experience configuring Service Account permissions and network URIs for Dataproc execution.

---

## 🖼️ Execution Evidence

### 1. Airflow Orchestration
The DAG manages task dependencies, ensuring the transformation only starts after data ingestion is complete.
![Airflow DAG](./Images/02.%20Graph_Apache_Airflow.png)

### 2. Cloud Processing (Dataproc)
Serverless execution of Spark jobs, showing the batch processing successfully completed.
![Dataproc Batch](./Images/03.%20Batches_Data_Proc.png)

### 3. Final Result in BigQuery
Preview of the `enriched_orders` table showing calculated fields like `total_price` and `price_tier`.
![BigQuery Results](./Images/04.%20Nueva_Tabla_Spark.png)

---

## 🔧 How to Run
1.  **Clone the repo.**
2.  **Setup GCP Credentials:** Place your Service Account JSON key in `include/keys/airflow-sa.json` (or your mapped directory).
3.  **Configure Environment:** Ensure your `.env` file points to the correct credentials path inside the container.
4.  **Start the Environment:**
    ```bash
    astro dev start
    ```
5.  **Set Airflow Variables:** Create the `gcpproject_id_new` variable in the Airflow UI.

---
