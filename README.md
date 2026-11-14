# Distributed Multi-Model Analytics for E-commerce Data

## 1. Project Overview

This project demonstrates how distributed and multi-model database technologies can be used to store, process, and analyse e-commerce data.

It combines MongoDB, Apache HBase, and Apache Spark to generate business insights about sales performance, customer value, product performance, and customer conversion behaviour.

## 2. Project Objectives

- Store and manage e-commerce data using MongoDB.
- Store customer browsing sessions using Apache HBase.
- Process and analyse large-scale structured and semi-structured data using Apache Spark.
- Perform integrated analytics to identify business trends and customer behaviour.
- Generate visualizations to support data-driven decision-making.

## 3. Technologies Used

- Python 3.11
- MongoDB 8.0
- Apache HBase
- Apache Spark 3.5.9
- Java 17
- Docker
- Git and GitHub
- Matplotlib

## 4. Project Structure

```text
Final Project Gisele/
│
├── data/
│   ├── users.json
│   ├── products.json
│   ├── categories.json
│   ├── transactions.json
│   └── sessions_0.json
│
├── scripts/
│   ├── import_mongodb.py
│   ├── mongodb_sales_analysis.js
│   ├── mongodb_customer_analysis.js
│   ├── load_hbase_sessions.py
│   ├── hbase_queries.hbase
│   ├── spark_processing.py
│   ├── integrated_analytics.py
│   ├── visualizations.py
│   └── validate_data.py
│
├── outputs/
│   └── Generated analytics results
│
├── report/
│   ├── Distributed_Multi_Model_Analytics_Report.pdf
│   ├── Distributed_Multi_Model_Analytics_Report.md
│   ├── screenshots/
│   └── Visualization charts
│
├── dataset_generator.py
├── dataset_generator_fixed.py
├── generator_clean.py
└── README.md
```

## 5. Dataset Description

The project uses generated e-commerce datasets:

| Dataset | Records |
|---|---:|
| Users | 100 |
| Products | 50 |
| Categories | 10 |
| Transactions | 100 |
| Customer sessions | 200 |

## 6. Database Implementation

### MongoDB

MongoDB stores:
- User profiles
- Product information
- Product categories
- Customer transactions

Aggregation pipelines are used to analyse sales performance by category and customer purchasing behaviour.

### Apache HBase

HBase stores customer browsing session records in a table named `customer_sessions`.

The table uses five column families:
- session
- activity
- device
- location
- conversion

A composite row key based on user ID, reverse timestamp, and session ID supports customer session retrieval.

## 7. Apache Spark Analytics

Apache Spark is used to:
- Load and clean JSON datasets.
- Transform transaction and session records.
- Perform Spark SQL queries.
- Analyse customer value.
- Evaluate product performance.
- Analyse sales trends.
- Calculate customer conversion rates.

## 8. Integrated Analytics

The project examines:
- Customer Lifetime Value indicators.
- Product affinity.
- Customer conversion funnel.
- Monthly sales trends.
- Customer journey behaviour.

The integrated Spark analysis combines transaction and session datasets. MongoDB and HBase are also used for their respective storage and query operations.

## 9. Visualizations

Four visualizations are generated:

1. Sales Overview
2. Customer Segmentation
3. Product Performance
4. Conversion Funnel

The charts are available in the `report/` folder.

## 10. Running the Project

Run the Python scripts from the project root directory after installing and configuring the required technologies.

Spark processing:

```powershell
python scripts/spark_processing.py
```

Integrated analytics:

```powershell
python scripts/integrated_analytics.py
```

Generate visualizations:

```powershell
python scripts/visualizations.py
```

MongoDB analysis scripts can be executed using `mongosh` after starting the MongoDB service.

HBase queries can be executed using the HBase shell after starting the HBase container.

## 11. Implementation Notes and Limitations

- Apache Spark was executed in local mode.
- HBase was configured in standalone mode using Docker.
- The integrated Spark analytics used the original JSON session and transaction datasets; direct Spark-to-HBase or Spark-to-MongoDB connector integration was not implemented.
- Customer value calculations are based on the available transaction records and are not predictive lifetime-value models.

## 12. Project Report

The complete project report, technical screenshots, architecture diagram, and visualizations are available in the `report/` directory.

## 13. Author

Gisele Ishimwe
**Project Title:** Distributed Multi-Model Analytics for E-commerce Data