package com.insite.streams.pii

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.insite.streams.common.Stream
import com.insite.streams.common.metrics.StreamMetrics
import com.insite.streams.common.utils.{KafkaUtils, SchemaMonitorUtils}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Stream implementation for PII data masking with schema monitoring
 */
class PIIStream extends Stream[PIIRecord] {
  private val logger = LoggerFactory.getLogger(getClass)
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  /**
   * Process records from Kafka, apply PII masking, and return processed stream
   */
  override def doLogic()(implicit env: StreamExecutionEnvironment, config: Map[String, String]): DataStream[PIIRecord] = {
    // Get schema path
    val schemaPath = config.getOrElse("schema-path",
      throw new IllegalArgumentException("schema-path configuration parameter is required"))

    // Refresh interval in minutes (default 1 minute)
    val refreshIntervalMinutes = config.getOrElse("schema.refresh.interval.minutes", "1").toInt

    // Create schema broadcast stream using SchemaMonitorUtils
    val schemaStream = SchemaMonitorUtils.createSchemaBroadcast(
      env,
      schemaPath,
      refreshIntervalMinutes
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
    // Get Kafka connection parameters
    val outputTopic = config.getOrElse("output-topic",
      throw new IllegalArgumentException("output-topic configuration parameter is required"))
    val bootstrapServers = config.getOrElse("bootstrap-servers", "localhost:9092")

    // Convert PIIRecord to JSON string
    val stringStream = stream.map(record => mapper.writeValueAsString(record.toMap))

    // Write to Kafka using KafkaUtils
    KafkaUtils.writeStringStream(
      stringStream,
      outputTopic,
      bootstrapServers
    )
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
  private class SchemaBroadcastProcessor extends BroadcastProcessFunction[String, JsonNode, PIIRecord] {
    override def processElement(
                                 value: String,
                                 ctx: BroadcastProcessFunction[String, JsonNode, PIIRecord]#ReadOnlyContext,
                                 out: Collector[PIIRecord]
                               ): Unit = {
      // Get current schema from broadcast state
      val schema = ctx.getBroadcastState(SchemaMonitorUtils.schemaDescriptor).get("current")

      if (schema != null) {
        try {
          // Parse input JSON
          val jsonNode = mapper.readTree(value)

          // Convert to PIIRecord
          val record = PIIRecord.fromJsonNode(jsonNode, Some(schema))

          // Apply PII masking based on schema
          val maskedRecord = maskPIIFields(record, schema)

          out.collect(maskedRecord)
        } catch {
          case e: Exception =>
            logger.error(s"Error processing record: ${e.getMessage}", e)
        }
      } else {
        logger.warn("Received record but schema is not yet available")
      }
    }

    override def processBroadcastElement(
                                          schema: JsonNode,
                                          ctx: BroadcastProcessFunction[String, JsonNode, PIIRecord]#Context,
                                          out: Collector[PIIRecord]
                                        ): Unit = {
      // Store schema in broadcast state
      ctx.getBroadcastState(SchemaMonitorUtils.schemaDescriptor).put("current", schema)
      logger.info("Schema updated in broadcast state")
    }
  }

  /**
   * Apply PII masking to all fields in a record based on schema
   */
  private def maskPIIFields(record: PIIRecord, schema: JsonNode): PIIRecord = {
    var processedRecord = record

    // Process each field in the record
    record.fields.foreach { case (fieldName, fieldValue) =>
      // Check if field is defined in schema
      SchemaPIIUtils.getFieldByName(schema, fieldName).foreach { fieldDef =>
        // Check if field is PII
        if (SchemaPIIUtils.isPIIField(fieldDef)) {
          // Apply PII operation
          val maskedValue = PIIOperations.applyPIIOperation(fieldValue, fieldDef)
          processedRecord = processedRecord.withField(fieldName, maskedValue)
        }
      }
    }

    processedRecord
  }
}