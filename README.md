# **insite-streams**

A scalable, modular stream processing framework for Flink applications with PII data masking and Iceberg integration, developed in Scala.


## **Project Overview**

The insite-streams project provides a comprehensive solution for event stream processing with a focus on:

- PII (Personally Identifiable Information) data protection

- Schema-driven configuration

- Data enrichment and transformation

- Iceberg table integration with upsert capabilities

- Time-based partitioning


## **Project Structure**

This project follows a modular architecture with three main components:

1. **common**: Core abstractions and utilities

   - Base Stream trait with doLogic/doOutput operations

   - Flink configuration utilities

   - Metrics collection framework

   - Schema handling utilities

2. **pii**: PII protection stream processor

   - Consumes events from Kafka

   - Uses schema-driven approach to identify PII fields

   - Applies appropriate masking/hashing based on schema definition

   - Outputs cleaned events to another Kafka topic

3. **enricher**: Data enrichment and Iceberg writer

   - Consumes cleaned events from the PII stream

   - Writes data to Iceberg tables with upsert semantics

   - Handles schema evolution dynamically

   - Implements time-based partitioning with multiple time dimensions:

     - Event date (when the event was created)

     - Processing timestamp

     - Processing date partition (event\_date\_prt)


## **Key Features**

### **Schema-Driven PII Processing**

- JSON schema defines field types, sizes, and PII flags

- Configurable PII treatment methods (masking, hashing, etc.)

- Field matching by name for flexible event structures


### **Dynamic Iceberg Integration**

- Upsert capability (update if exists, insert if new)

- Daily partitioning for efficient data management

- Automatic schema evolution to handle new fields


### **Partitioning Strategy**

The enricher implements a sophisticated time-based partitioning strategy with:

- event\_date: Original timestamp when the event occurred

- processing\_timestamp: Exact time when the record was processed

- event\_date\_prt: Partition date for the data (daily partitioning)


## **Getting Started**

### **Prerequisites**

- Java 8+

- Scala 2.12+

- Maven

- Apache Flink

- Kafka cluster

- Iceberg-compatible storage (e.g., HDFS, S3)


### **Building the Project**

    mvn clean package


### **Running the PII Streamer**

    flink run -c com.insite.streams.pii.PIIStream insite-streams-pii.jar \
      --input-topic source-events \
      --output-topic cleaned-events \
      --schema-path /path/to/schema.json


### **Running the Enricher**

    flink run -c com.insite.streams.enricher.EnricherStream insite-streams-enricher.jar \
      --input-topic cleaned-events \
      --iceberg-catalog hadoop_catalog \
      --iceberg-database events_db \
      --iceberg-table processed_events


## **Configuration**

### **PII Schema Example**

    {
      "fields": [
        {
          "name": "id",
          "type": "string",
          "size": 36,
          "isPII": false
        },
        {
          "name": "email",
          "type": "string",
          "size": 100,
          "isPII": true,
          "piiOperation": "mask"
        },
        {
          "name": "age",
          "type": "integer",
          "isPII": true,
          "piiOperation": "generalize",
          "ranges": [0, 18, 35, 50, 65, 100]
        }
      ]
    }


## **Module Responsibilities**

### **Common Module**

- Provides the Stream trait defining the processing contract

- Offers utilities for Flink configuration, metrics, and schema handling

- Implements reusable PII transformation functions


### **PII Module**

- Reads events from Kafka source topic

- Loads and interprets the schema definition

- Applies PII transformations based on schema

- Writes protected data to output Kafka topic


### **Enricher Module**

- Reads protected events from Kafka

- Creates or updates Iceberg table schema as needed

- Implements upsert logic for Iceberg writes

- Handles partitioning by date with multiple time dimensions


## **Development**

This project is developed in Scala to leverage its concise syntax, functional programming capabilities, and strong integration with Apache Flink, while using Maven for build management. Scala provides several advantages for stream processing applications:

- Expressive and concise syntax for complex data transformations

- Strong type system that catches errors at compile time

- Built-in support for immutable data structures

- Native pattern matching for clean, readable code

- Seamless integration with Java libraries and frameworks

- First-class support in Apache Flink

To add a new PII transformation technique, extend the PIIOperations trait in the common module and implement your custom transformation logic.

To modify partitioning strategy, adjust the PartitioningStrategy trait in the enricher module.

