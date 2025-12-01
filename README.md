# Real-Time-Streaming-Pipeline# Real-Time Syslog Streaming Pipeline

[![syslog-ng](https://img.shields.io/badge/syslog--ng-Log%20Collector-0A6D92?style=for-the-badge&logo=syslog-ng)](https://www.syslog-ng.com/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-Streaming-000000?style=for-the-badge&logo=apachekafka)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache_Spark-Structured_Streaming-E25A1C?style=for-the-badge&logo=apachespark)](https://spark.apache.org/)
[![HDFS](https://img.shields.io/badge/Hadoop-HDFS-66C2FF?style=for-the-badge&logo=apachehadoop)](https://hadoop.apache.org/)
[![Zookeeper](https://img.shields.io/badge/Apache_Zookeeper-Coordination-F28D00?style=for-the-badge&logo=apachezookeeper)](https://zookeeper.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker)](https://www.docker.com/)
[![PySpark](https://img.shields.io/badge/Python-PySpark-3776AB?style=for-the-badge&logo=python&logoColor=yellow)](https://spark.apache.org/docs/latest/api/python/)

A distributed **real-time log analytics pipeline** built with **syslog-ng → Kafka → Spark Structured Streaming → HDFS**.

Perfect for production monitoring, security event analysis (SIEM), centralized logging, and scalable log ingestion platforms.

![Architecture](https://via.placeholder.com/800x400.png?text=Real-Time+Syslog+Pipeline+Architecture)  
*(Diagram below in text form)*

## 📌 Overview

This project implements a complete end-to-end real-time log processing system:

- Logs from servers/applications → collected by **syslog-ng**
- Forwarded reliably to **Apache Kafka**
- Consumed and processed in real-time using **Spark Structured Streaming**
- Cleaned, enriched, and stored in **HDFS** (Parquet format, partitioned by date/hour)

The pipeline is fully containerized with Docker Compose for easy deployment and testing.

## 🏗️ Architecture

```text
[ Applications / System Logs ]
               │
               ▼
          syslog-ng
               │ (TCP/UDP)
               ▼
           Kafka Topic (syslog-stream)
               │ (stream)
               ▼
   Spark Structured Streaming
               │ (processed stream)
               ▼
             HDFS (Parquet, partitioned)