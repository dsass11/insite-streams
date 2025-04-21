package com.insite.streams.pii

import com.fasterxml.jackson.databind.JsonNode
import com.insite.streams.common.utils.JsonUtils.mapper
import com.insite.streams.common.Stream
import com.insite.streams.common.metrics.StreamMetrics
import com.insite.streams.common.utils.{KafkaUtils, SchemaMonitorUtils}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}


/**
 * Stream implementation for PII data masking with schema monitoring
 */
class PIIStream extends Stream[PIIRecord] with java.io.Serializable {
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * Process records from Kafka, apply PII masking, and return processed stream
   */
  override def doLogic()(implicit env: StreamExecutionEnvironment, config: Map[String, String]): DataStream[PIIRecord] = {
    // Get schema path
    val schemaPath = config.getOrElse("schema-path",
      throw new IllegalArgumentException("schema-path configuration parameter is required"))

    // Get refresh interval in seconds (default 60)
    val refreshIntervalSeconds = config.getOrElse("schema.refresh.interval.seconds", "60").toInt

    // Determine if verbose logging is enabled
    val verboseLogging = config.getOrElse("schema.verbose.logging", "false").toBoolean

    // Create schema broadcast stream using the updated SchemaMonitorUtils
    val schemaStream = SchemaMonitorUtils.createSchemaBroadcast(
      env,
      schemaPath,
      refreshIntervalSeconds,
      verboseLogging
    )


    // Get Kafka connection parameters
    val inputTopic = config.getOrElse("input-topic",
      throw new IllegalArgumentException("input-topic configuration parameter is required"))
    val bootstrapServers = config.getOrElse("bootstrap-servers", "localhost:9092")
    val groupId = config.getOrElse("group-id", "pii-processor")

    // Read from Kafka using KafkaUtils
    val inputStream = KafkaUtils.readStringStream(
      inputTopic,
      env,
      bootstrapServers,
      groupId
    )

    // Connect input stream with schema broadcast and process
    val processedStream = inputStream
      .connect(schemaStream)
      .process(new SchemaBroadcastProcessor())
      .name("PII-Processing")

    processedStream
  }

  /**
   * Output the processed stream to Kafka
   */
  override def doOutput(stream: DataStream[PIIRecord])(implicit env: StreamExecutionEnvironment, config: Map[String, String]): Unit = {
    try {
      // Get Kafka connection parameters
      val outputTopic = config.getOrElse("output-topic",
        throw new IllegalArgumentException("output-topic configuration parameter is required"))
      val bootstrapServers = config.getOrElse("bootstrap-servers", "localhost:9092")

      logger.info(s"Setting up output to Kafka topic: $outputTopic, bootstrap servers: $bootstrapServers")

      // Convert PIIRecord to JSON string
      val stringStream = stream.map(record => {
        try {
          logger.info(s"Converting record to JSON: ${record.id}")
          val json = mapper.writeValueAsString(record.toMap)
          logger.info(s"Record converted to JSON: ${json.take(100)}...")
          json
        } catch {
          case e: Exception =>
            logger.error(s"Error converting record to JSON: ${e.getMessage}", e)
            s"""{"error":"Failed to convert record","id":"${record.id}"}"""
        }
      })

      // Write to Kafka using KafkaUtils
      logger.info("Writing to Kafka...")
      KafkaUtils.writeStringStream(
        stringStream,
        outputTopic,
        bootstrapServers
      )
      logger.info("Kafka sink configured")
    } catch {
      case e: Exception =>
        logger.error(s"Error in doOutput: ${e.getMessage}", e)
    }
  }

  /**
   * Collect metrics for PII processing
   */
  override def collectMetrics(stream: DataStream[PIIRecord], metrics: StreamMetrics)
                             (implicit env: StreamExecutionEnvironment, config: Map[String, String]): StreamMetrics = {
    // Since schema is managed dynamically, just add a simple metric
    metrics.addBusinessMetric("schema_monitoring_enabled", true)
  }

  /**
   * Broadcast processor that applies schema rules to incoming records
   */
  private class SchemaBroadcastProcessor extends BroadcastProcessFunction[String, JsonNode, PIIRecord] with java.io.Serializable {
    @transient private lazy val processorLogger = LoggerFactory.getLogger(classOf[SchemaBroadcastProcessor])

    override def processElement(
                                 value: String,
                                 ctx: BroadcastProcessFunction[String, JsonNode, PIIRecord]#ReadOnlyContext,
                                 out: Collector[PIIRecord]
                               ): Unit = {
      // Log the raw input
      processorLogger.info(s"Received raw input: ${value.take(100)}...")

      // Get current schema from broadcast state
      val schema = ctx.getBroadcastState(SchemaMonitorUtils.schemaDescriptor).get("current")

      if (schema != null) {
        try {
          // Parse input JSON
          processorLogger.info("Attempting to parse JSON")
          val jsonNode = mapper.readTree(value)
          processorLogger.info(s"JSON parsed, has id field: ${jsonNode.has("id")}")

          // Get all defined field names from the schema
          val definedFields = SchemaPIIUtils.getFields(schema).map(field =>
            Option(field.get("name")).map(_.asText()).getOrElse("")
          ).toSet + "id" // Include "id" which is required

          // Get all field names from the JSON
          val allJsonFields = {
            val fields = new scala.collection.mutable.HashSet[String]()
            val iterator = jsonNode.fieldNames()
            while (iterator.hasNext) {
              fields += iterator.next()
            }
            fields.toSet
          }

          // Find fields in JSON but not in schema
          val undefinedFields = allJsonFields -- definedFields

          // Log warning for undefined fields
          if (undefinedFields.nonEmpty) {
            processorLogger.warn(s"Event contains fields not defined in schema: ${undefinedFields.mkString(", ")}")
          }

          // Convert to PIIRecord (only includes schema-defined fields)
          processorLogger.info("Converting to PIIRecord")
          val record = PIIRecord.fromJsonNode(jsonNode, Some(schema))
          processorLogger.info(s"Successfully created PIIRecord with ID: ${record.id}")

          // Apply PII masking based on schema
          processorLogger.info("Applying PII masking")
          val maskedRecord = SchemaPIIUtils.maskPIIFields(record, schema)
          processorLogger.info(s"Successfully masked fields: ${maskedRecord.fields.keys.mkString(", ")}")

          // Output the processed record
          processorLogger.info("Collecting processed record")
          out.collect(maskedRecord)
          processorLogger.info("Record has been collected for output")
        }
        catch {
          case e: Exception =>
            processorLogger.error(s"Error processing record: ${e.getMessage}", e)
        }
      }
      else {
        processorLogger.warn("Received record but schema is not yet available")
      }
    }

    override def processBroadcastElement(
                                          schema: JsonNode,
                                          ctx: BroadcastProcessFunction[String, JsonNode, PIIRecord]#Context,
                                          out: Collector[PIIRecord]
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