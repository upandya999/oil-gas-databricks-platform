# Oil & Gas Data Platform (Azure + Databricks)

## Overview
This project simulates a production-grade Oil & Gas data platform handling millions of sensor records.

## Problem Statement
Oil & Gas companies need to monitor well performance and sensor readings at scale to detect anomalies and optimize production.

This pipeline ingests, processes, and aggregates large-scale sensor data using a medallion architecture.

---

## Architecture

```mermaid
flowchart TD

A[Sensor Data Generation - 10M rows] --> B[Bronze - Raw Delta]
B --> C[Silver - Cleaned Data]
C --> D[Gold - Aggregated Data]
D --> E[BI / Analytics]

F[Airflow DAG] --> A
F --> B
F --> C
F --> D

G[Azure Databricks] --> B
G --> C
G --> D