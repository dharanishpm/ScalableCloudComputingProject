# Real-Time E-commerce User Behaviour Analytics

## Problem Statement

E-commerce companies need to answer questions such as:

- Which products are trending right now?
- Which categories receive the most impressions?
- Which advertisements have the highest click-through rate (CTR)?
- Are users suddenly abandoning a product?
- Which products are receiving unusually high traffic?

A batch-only system cannot answer these questions immediately, while a streaming-only system cannot provide accurate historical analytics. Lambda Architecture combines both.

## Features

- **Real-Time Streaming Analytics**: Ingest and process user interactions (clicks, views, purchases) in real-time.
- **Historical Batch Analytics**: Process large volumes of historical data for complex aggregations and reporting.
- **Lambda Architecture**: Seamlessly integrate real-time and batch views to provide a unified data API.
- **Scalable Ingestion**: Handle high-throughput e-commerce traffic events.

## Proposed Architecture

The system follows a standard Lambda Architecture with three main layers:
1. **Batch Layer**: Manages the master dataset (immutable, append-only) and pre-computes batch views.
2. **Speed Layer**: Deals with recent data only. Processes data streams in real-time to minimize latency.
3. **Serving Layer**: Indexes batch views and speed views so that they can be queried with low latency.

## Technology Stack

- **Data Ingestion**: Apache Kafka
- **Stream Processing**: Apache Flink / Spark Streaming
- **Batch Processing**: Apache Spark / Hadoop MapReduce
- **Serving Database**: Apache Cassandra / Elasticsearch
- **Storage**: HDFS / Amazon S3
