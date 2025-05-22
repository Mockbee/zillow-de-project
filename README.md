# zillow-data-engineering-project

# 🏡 Zillow Data Engineering Project

This project demonstrates the design and implementation of a modern data pipeline using **Databricks**, **Azure Blob Storage**, and Zillow real estate data. The pipeline is structured using the **Medallion Architecture** (Bronze → Silver → Gold) to enable scalable and reliable processing.

---

### 📌 Project Architecture

![Architecture Diagram](https://github.com/Mockbee/zillow-de-project/blob/main/Zillow%20Data%20Architecture%20Diagram.png)

---

### 📦 Data Source

We integrate with publicly available **Zillow housing data**, which includes:

- **Property Information:** Core housing attributes such as ID, address, coordinates, and type.
- **Tax & Price Data:** Historical pricing and taxation records for properties.

The raw datasets are fetched from APIs and processed through the Medallion architecture layers.

---

### 🛠️ Tools & Services Used

| Tool/Service         | Role                                                                 |
|----------------------|----------------------------------------------------------------------|
| **Databricks**       | Notebook-based development, orchestration, transformation (PySpark) |
| **Azure Blob Storage** | Stores raw (Bronze), cleaned (Silver), and curated (Gold) data       |
| **Delta Lake**       | ACID-compliant storage for reliable data operations                 |
| **Databricks Workflows** | Orchestrates multi-stage job execution and scheduling            |

---

### 🧱 Medallion Architecture Layers

1. **Bronze Layer (Raw Ingestion):**
   - Ingests raw Zillow data (property, price, tax) in JSON/CSV.
   - Stores exact payload for traceability and recovery.

2. **Silver Layer (Transformation):**
   - Parses, flattens, and enriches the data using PySpark.
   - Adds metadata and ensures type consistency.

3. **Gold Layer (Enriched Data):**
   - Outputs business-ready tables for analytics, e.g., pricing trends by region.

---

### 🔁 Pipeline Flow

The project is orchestrated with **Databricks Workflows**, which executes tasks in a DAG structure:
1. [FetchingPropertyAPI] and [FetchingTaxandPriceData]
        ↓
2. [Bronze_Layer_Property], [Bronze_Layer_Price], [Bronze_Layer_Tax]
        ↓
3. [check_for_property_data], [check_for_price_data], [check_for_tax_data]
        ↓
4. [silver_layer_property], [silver_layer_price], [silver_layer_tax]
        ↓
5. [Move_to_archive]

