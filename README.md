**SFTP to GCS File Ingestion Pipeline**
Overview

This project demonstrates an Airflow-based ETL flow that extracts CSV files from an external SFTP system and uploads them to Google Cloud Storage (GCS).

**Process**
Read pipeline configuration from CONFIG_TABLE.
Connect to the external SFTP server using the configured SFTP Connection ID.
Identify files matching Sales*.csv.
Download files to the temporary path /tmp/IN_SFTP.
Connect to GCS using the configured GCS Connection ID.
Upload the files to the configured GCS bucket path.

**Technologies**
Python
Apache Airflow
SFTP
Google Cloud Storage
SQL
GCP
Key Features
Configuration-driven ETL
Automated SFTP file extraction
Temporary file staging
GCS integration
Airflow orchestration
Error handling and retries
