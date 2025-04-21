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

# Flink PII Stream Playground

A comprehensive testing environment for Flink stream processing applications with focus on PII data masking and dynamic schema handling.

## Key Components

This playground environment enables testing of several important aspects of Flink applications:

### 1. PII Data Masking

The PIIStream application applies different masking techniques to PII (Personally Identifiable Information) fields based on schema definitions:
- **Mask:** Partially obscures data with asterisks (`*`)
- **Hash:** Replaces values with secure hashes
- **Redact:** Completely replaces values with `[REDACTED]`
- **Generalize:** Groups numeric values into ranges
- **Randomize:** Modifies values while preserving scale

### 2. Schema Monitoring and Broadcasting

The application uses schema files to define which fields contain PII and how they should be masked:
- Schemas are loaded and broadcast to all processing nodes
- Schema changes are detected and applied dynamically
- The system handles mismatches between schema and data gracefully

### 3. Serialization and Checkpoint Management

The playground demonstrates proper serialization practices for Flink applications:
- Classes implement `java.io.Serializable` where needed
- Transient fields are marked with `@transient` to avoid serialization issues
- Non-serializable components like loggers use `lazy val` initialization

### 4. Checkpoint & Restart Behavior

The application uses Flink's checkpointing to ensure reliable processing:
- Kafka consumer offsets are committed with checkpoints
- On restart, processing resumes from last committed offset
- No data loss or duplicate processing occurs

## Setup & Usage Guide

### Prerequisites

- Docker and Docker Compose
- Maven
- Java 8 or higher

### Initial Setup

1. Clone the repository and navigate to the playground directory:
   ```bash
   cd playground
   ```

2. Start the Kafka environment:
   ```bash
   cd docker
   docker-compose down
   docker-compose up -d
   cd ..
   ```

3. Initialize the playground (create topics, etc.):
   ```bash
   ./scripts/init-playground.sh
   ```

### Testing Workflow

1. **Open multiple terminal windows:**
    - Terminal 1: Run the Flink application
    - Terminal 2: Send events
    - Terminal 3: Monitor input topic
    - Terminal 4: Monitor output topic

2. **Terminal 1: Start the Flink application:**
   ```bash
   ./scripts/run-with-explicit-deps.sh schema.refresh.interval.seconds=300 checkpoint.interval=10000 checkpoint.dir=file:///tmp/flink-checkpoints
   ```

3. **Terminal 3: Monitor Kafka input topic:**
   ```bash
   docker exec kafka kafka-console-consumer --topic pii-input --bootstrap-server kafka:9092 --from-beginning
   ```

4. **Terminal 4: Monitor Kafka output topic:**
   ```bash
   docker exec kafka kafka-console-consumer --topic pii-output --bootstrap-server kafka:9092 --from-beginning
   ```

5. **Terminal 2: Send a test event:**
   ```bash
   ./scripts/send-event.sh events/sample-event.json
   ```

### Testing Schema Updates

1. Examine current schema:
   ```bash
   cat schemas/pii-schema.json
   ```

2. Send an event and observe the masking in the output topic

3. Modify the schema (e.g., change a field from isPII=true to isPII=false)
   ```bash
   # Make a copy of the original schema first
   cp schemas/pii-schema.json schemas/pii-schema-original.json
   
   # Edit the schema file
   nano schemas/pii-schema.json
   ```

4. Wait for the refresh interval (5 minutes by default)

5. Send another event and observe the different masking behavior

### Testing Restart Behavior

1. Send a test event and verify it appears in the output topic

2. Stop the Flink application (Ctrl+C in Terminal 1)

3. Send another event (it won't be processed yet)

4. Restart the Flink application
   ```bash
   ./scripts/run-with-explicit-deps.sh schema.refresh.interval.seconds=300 checkpoint.interval=10000 checkpoint.dir=file:///tmp/flink-checkpoints
   ```

5. Observe that:
    - The application resumes from where it left off
    - The event sent while the app was down gets processed
    - Previously processed events aren't reprocessed

## Understanding Key Technical Concepts

### Schema Broadcasting

The application uses Flink's broadcast state pattern to distribute schema information:
- Schema is loaded from a file and broadcast to all parallel instances
- Each processing node receives schema updates through broadcast variables
- The `SchemaMonitorUtils` class handles periodic checking for schema changes

### Serialization in Flink

Proper serialization is critical for distributed processing:
- Flink operators are distributed across a cluster, requiring serialization
- Non-serializable fields (loggers, connections) must be marked `@transient`
- Initialization of transient fields should use `lazy val` for on-demand creation

### Checkpointing and State Management

Checkpointing ensures exactly-once processing semantics:
- Regular snapshots of application state and Kafka offsets
- Resuming from the last successful checkpoint after failures
- Using `OffsetsInitializer.committedOffsets()` for the Kafka consumer

### Schema-Event Mismatch Handling

The application implements a conservative approach to mismatches:
- Fields in the event but not in schema: Logged as warnings but not processed
- Fields in schema but not in event: Treated as null/missing values
- Dynamic properties: Can be collected but not included in standard processing

## Troubleshooting

### Kafka Connection Issues
If you see "LEADER_NOT_AVAILABLE" errors:
```bash
# Make sure topics exist
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092
# Create topics if needed
docker exec kafka kafka-topics --create --if-not-exists --topic pii-input --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1
```

### Schema Not Found
If you see "Schema file not found" errors:
```bash
# Check schema file location
ls -la schemas/
# Copy schema file if needed
cp events/sample-schema.json schemas/pii-schema.json
```

### Checkpoint Errors
If checkpointing fails:
```bash
# Try with AT_LEAST_ONCE instead of EXACTLY_ONCE semantics
./scripts/run-with-explicit-deps.sh schema.refresh.interval.seconds=300 checkpoint.interval=10000 checkpoint.dir=file:///tmp/flink-checkpoints checkpoint.mode=at_least_once
```

### Serialization Errors
If you see "not serializable" errors in logs, check:
- The class implements `java.io.Serializable`
- Non-serializable fields are marked `@transient`
- Any nested or inner classes are also serializable

## Adding a New Stream

The playground is designed to be extended with additional stream processing applications. Here's how to create a new stream based on the common module:

### 1. Create a New Module

First, add your new module to the parent pom.xml:

```xml
<modules>
    <module>common</module>
    <module>pii</module>
    <module>your-new-module</module>
</modules>
```

Create a directory for your new module with its own pom.xml:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.insite</groupId>
        <artifactId>insite-streams</artifactId>
        <version>1.0-SNAPSHOT</version>
    </parent>

    <artifactId>your-new-module</artifactId>

    <dependencies>
        <dependency>
            <groupId>com.insite</groupId>
            <artifactId>common</artifactId>
            <version>${project.version}</version>
        </dependency>
        
        <!-- Add any additional dependencies your stream needs -->
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

### 2. Implement Your Stream Class

Create a new stream class that extends the `Stream` trait from the common module:

```scala
package com.insite.streams.yournewstream

import com.fasterxml.jackson.databind.JsonNode
import com.insite.streams.common.Stream
import com.insite.streams.common.metrics.StreamMetrics
import com.insite.streams.common.utils.{JsonUtils, KafkaUtils, SchemaMonitorUtils}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class YourNewStream extends Stream[YourRecordType] with java.io.Serializable {
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  override def doLogic()(implicit env: StreamExecutionEnvironment, config: Map[String, String]): DataStream[YourRecordType] = {
    // Read configuration parameters
    val schemaPath = config.getOrElse("schema-path",
      throw new IllegalArgumentException("schema-path configuration parameter is required"))
    val refreshIntervalSeconds = config.getOrElse("schema.refresh.interval.seconds", "60").toInt
    val verboseLogging = config.getOrElse("schema.verbose.logging", "false").toBoolean

    // Create schema broadcast stream using SchemaMonitorUtils
    val schemaStream = SchemaMonitorUtils.createSchemaBroadcast(
      env,
      schemaPath,
      refreshIntervalSeconds,
      verboseLogging
    )

    // Read from Kafka
    val inputTopic = config.getOrElse("input-topic",
      throw new IllegalArgumentException("input-topic configuration parameter is required"))
    val bootstrapServers = config.getOrElse("bootstrap-servers", "localhost:9092")
    val groupId = config.getOrElse("group-id", "your-processor")

    val inputStream = KafkaUtils.readStringStream(
      inputTopic,
      env,
      bootstrapServers,
      groupId
    )

    // Process the stream
    val processedStream = inputStream
      .connect(schemaStream)
      .process(new YourBroadcastProcessor())
      .name("Your-Processing")

    processedStream
  }

  override def doOutput(stream: DataStream[YourRecordType])(implicit env: StreamExecutionEnvironment, config: Map[String, String]): Unit = {
    // Get output configuration
    val outputTopic = config.getOrElse("output-topic",
      throw new IllegalArgumentException("output-topic configuration parameter is required"))
    val bootstrapServers = config.getOrElse("bootstrap-servers", "localhost:9092")

    // Convert your record type to string and send to Kafka
    val stringStream = stream.map(record => JsonUtils.mapper.writeValueAsString(record))

    // Write to Kafka
    KafkaUtils.writeStringStream(
      stringStream,
      outputTopic,
      bootstrapServers
    )
  }

  override def collectMetrics(stream: DataStream[YourRecordType], metrics: StreamMetrics)
                            (implicit env: StreamExecutionEnvironment, config: Map[String, String]): StreamMetrics = {
    // Add custom metrics
    metrics.addBusinessMetric("your_metric", "your_value")
  }

  // Define your processor
  private class YourBroadcastProcessor extends BroadcastProcessFunction[String, JsonNode, YourRecordType] with java.io.Serializable {
    @transient private lazy val processorLogger = LoggerFactory.getLogger(classOf[YourBroadcastProcessor])

    override def processElement(
        value: String,
        ctx: BroadcastProcessFunction[String, JsonNode, YourRecordType]#ReadOnlyContext,
        out: Collector[YourRecordType]
    ): Unit = {
      // Your processing logic here
    }

    override def processBroadcastElement(
        schema: JsonNode,
        ctx: BroadcastProcessFunction[String, JsonNode, YourRecordType]#Context,
        out: Collector[YourRecordType]
    ): Unit = {
      // Get current schema
      val currentSchema = ctx.getBroadcastState(SchemaMonitorUtils.schemaDescriptor).get("current")

      // Only log when schema actually changes
      if (currentSchema == null || !currentSchema.equals(schema)) {
        processorLogger.info("Schema updated in broadcast state with new content")
      }

      // Always update the broadcast state
      ctx.getBroadcastState(SchemaMonitorUtils.schemaDescriptor).put("current", schema)
    }
  }
}
```

### 3. Create Your Record Type

Define your record type for the stream:

```scala
package com.insite.streams.yournewstream

case class YourRecordType(
  id: String,
  fields: Map[String, Any] = Map.empty
) {
  def getField[T](name: String): Option[T] = {
    fields.get(name).flatMap {
      case value: T => Some(value)
      case _ => None
    }
  }
  
  def withField(name: String, value: Any): YourRecordType = {
    this.copy(fields = fields + (name -> value))
  }
  
  def toMap: Map[String, Any] = {
    Map("id" -> id) ++ fields
  }
}
```

### 4. Create Processing Logic

Create the specific processing logic for your stream. For example, if you're implementing a validation stream:

```scala
package com.insite.streams.yournewstream

import com.fasterxml.jackson.databind.JsonNode

object YourProcessingUtils {
  /**
   * Validate fields against schema types
   */
  def validateFields(record: YourRecordType, schema: JsonNode): YourRecordType = {
    // Get fields from schema
    val schemaFields = getAllFields(schema)
    
    // Track validation results
    val validationResults = collection.mutable.Map[String, Boolean]()
    
    // Validate each field
    schemaFields.foreach { fieldDef =>
      val fieldName = Option(fieldDef.get("name")).map(_.asText()).getOrElse("")
      val fieldType = Option(fieldDef.get("type")).map(_.asText()).getOrElse("string")
      
      if (fieldName.nonEmpty) {
        record.getField[Any](fieldName) match {
          case Some(value) => 
            val isValid = validateFieldType(value, fieldType)
            validationResults(fieldName) = isValid
          case None =>
            // Field in schema but not in record - mark as valid but missing
            validationResults(fieldName) = true
        }
      }
    }
    
    // Add validation results to record
    record.copy(fields = record.fields ++ Map("__validationResults" -> validationResults.toMap))
  }
  
  private def validateFieldType(value: Any, expectedType: String): Boolean = {
    expectedType.toLowerCase match {
      case "string" => value.isInstanceOf[String]
      case "integer" | "int" => value.isInstanceOf[Int] || value.isInstanceOf[Long]
      case "double" | "float" => value.isInstanceOf[Double] || value.isInstanceOf[Float]
      case "boolean" => value.isInstanceOf[Boolean]
      case _ => true // For unknown types, consider valid
    }
  }
  
  private def getAllFields(schema: JsonNode): List[JsonNode] = {
    val fields = schema.get("fields")
    if (fields == null || !fields.isArray) {
      List.empty
    } else {
      val result = new collection.mutable.ListBuffer[JsonNode]
      val iterator = fields.elements()
      while (iterator.hasNext) {
        result += iterator.next()
      }
      result.toList
    }
  }
}
```

### 5. Update Your Broadcast Processor

Update your processor to use the validation logic:

```scala
override def processElement(
  value: String,
  ctx: BroadcastProcessFunction[String, JsonNode, YourRecordType]#ReadOnlyContext,
  out: Collector[YourRecordType]
): Unit = {
  // Log the raw input
  processorLogger.info(s"Received raw input: ${value.take(100)}...")

  // Get current schema from broadcast state
  val schema = ctx.getBroadcastState(SchemaMonitorUtils.schemaDescriptor).get("current")

  if (schema != null) {
    try {
      // Parse input JSON
      val jsonNode = JsonUtils.mapper.readTree(value)
      
      // Check for ID field
      if (!jsonNode.has("id")) {
        processorLogger.error("Input JSON missing ID field, skipping")
        return
      }
      
      // Extract fields from JSON
      val fields = new collection.mutable.HashMap[String, Any]()
      val fieldIterator = jsonNode.fields()
      while (fieldIterator.hasNext) {
        val entry = fieldIterator.next()
        val fieldName = entry.getKey
        val fieldValue = entry.getValue
        
        if (fieldName != "id") {
          // Extract value based on type
          val value = if (fieldValue.isTextual) {
            fieldValue.asText()
          } else if (fieldValue.isInt) {
            fieldValue.asInt()
          } else if (fieldValue.isLong) {
            fieldValue.asLong()
          } else if (fieldValue.isDouble) {
            fieldValue.asDouble()
          } else if (fieldValue.isBoolean) {
            fieldValue.asBoolean()
          } else if (fieldValue.isNull) {
            null
          } else {
            // For complex types, just store as string
            fieldValue.toString
          }
          
          fields(fieldName) = value
        }
      }
      
      // Create record
      val record = YourRecordType(jsonNode.get("id").asText(), fields.toMap)
      
      // Apply validation
      val validatedRecord = YourProcessingUtils.validateFields(record, schema)
      
      // Output the processed record
      out.collect(validatedRecord)
      
    } catch {
      case e: Exception =>
        processorLogger.error(s"Error processing record: ${e.getMessage}", e)
    }
  } else {
    processorLogger.warn("Received record but schema is not yet available")
  }
}
```

### 6. Add Run Script for Your Stream

Create a script in the playground directory to run your new stream:

```bash
#!/bin/bash

# Determine script and directory locations
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLAYGROUND_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$PLAYGROUND_DIR")"

# Go to project root
cd "$PROJECT_ROOT"
echo "Building project at $(pwd)..."

# Build the project
mvn clean package -DskipTests

# Create Kafka topics
cd "$PLAYGROUND_DIR"
echo "Setting up Kafka topics..."
docker exec kafka kafka-topics --create --if-not-exists --topic validation-input --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1
docker exec kafka kafka-topics --create --if-not-exists --topic validation-output --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1

# Run the YourNewStream application
cd "$PROJECT_ROOT"
echo "Starting YourNewStream application from $(pwd)..."
java -cp your-new-module/target/your-new-module-1.0-SNAPSHOT.jar com.insite.streams.common.StreamRunner \
  com.insite.streams.yournewstream.YourNewStream \
  bootstrap-servers=localhost:29092 \
  input-topic=validation-input \
  output-topic=validation-output \
  schema-path="$PLAYGROUND_DIR/schemas/validation-schema.json" \
  group-id=validation-processor-1 \
  parallelism=1 \
  schema.refresh.interval.seconds=300 \
  checkpoint.interval=10000 \
  checkpoint.dir=file:///tmp/flink-checkpoints
```

### 7. Create Schema File

Create a schema file for your new stream:

```json
{
  "name": "ValidationSchema",
  "version": "1.0",
  "description": "Schema for data validation",
  "fields": [
    {
      "name": "id",
      "type": "string",
      "description": "Unique identifier",
      "required": true
    },
    {
      "name": "name",
      "type": "string",
      "description": "User name"
    },
    {
      "name": "age",
      "type": "integer",
      "description": "User age"
    },
    {
      "name": "active",
      "type": "boolean",
      "description": "Whether the user is active"
    }
  ]
}
```

### 8. Test Your New Stream

Follow the same testing workflow as with the PII stream:

1. Open multiple terminals
2. Start your new stream
3. Send test events to your input topic
4. Monitor the output topic for validated records

### 9. Reusing Common Components

The common module provides several reusable components:

- **Stream Trait**: Base interface for all stream processors
- **SchemaMonitorUtils**: Handles schema loading and broadcasting
- **KafkaUtils**: Provides utilities for Kafka input/output
- **FlinkUtils**: Configures the Flink execution environment
- **StreamMetrics**: Collects metrics for monitoring

These components simplify creating new stream applications by providing common functionality that you can build upon.

