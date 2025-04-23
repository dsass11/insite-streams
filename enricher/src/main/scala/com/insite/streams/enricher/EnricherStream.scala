package com.insite.streams.enricher

import com.fasterxml.jackson.databind.JsonNode
import com.insite.streams.common.Stream
import com.insite.streams.common.metrics.StreamMetrics
import com.insite.streams.common.utils.{JsonUtils, KafkaUtils, SchemaMonitorUtils}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}
import java.util.concurrent.atomic.AtomicLong

/**
 * Stream implementation for data enrichment and Iceberg integration
 */
class EnricherStream extends Stream[EnricherRecord] with java.io.Serializable {
  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  // Counters for metrics
  @transient private lazy val totalRecordsCounter = new AtomicLong(0)
  @transient private lazy val validRecordsCounter = new AtomicLong(0)
  @transient private lazy val invalidRecordsCounter = new AtomicLong(0)

  /**
   * Process records from Kafka, apply validation, and return processed stream
   */
  override def doLogic()(implicit env: StreamExecutionEnvironment, config: Map[String, String]): DataStream[EnricherRecord] = {
    // Get schema path
    val schemaPath = config.getOrElse("schema-path",
      throw new IllegalArgumentException("schema-path configuration parameter is required"))

    // Get refresh interval in seconds (default 60)
    val refreshIntervalSeconds = config.getOrElse("schema.refresh.interval.seconds", "60").toInt

    // Determine if verbose logging is enabled
    val verboseLogging = config.getOrElse("schema.verbose.logging", "false").toBoolean

    // Create schema broadcast stream using SchemaMonitorUtils
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
    val groupId = config.getOrElse("group-id", "enricher-processor")

    // Read from Kafka
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
      .name("Enricher-Processing")

    // Only include records that pass validation if configured
    val filterInvalid = config.getOrElse("filter.invalid.records", "true").toBoolean
    
    if (filterInvalid) {
      processedStream
        .filter(record => record.validationStatus.values.forall(valid => valid))
        .name("Filter-Valid-Records")
    } else {
      processedStream
    }
  }

  /**
   * Output the processed stream to Iceberg
   */
  override def doOutput(stream: DataStream[EnricherRecord])(implicit env: StreamExecutionEnvironment, config: Map[String, String]): Unit = {
    try {
      logger.info("Setting up output to Iceberg")
      
      // Use IcebergSinkUtils to write to Iceberg
      IcebergSinkUtils.writeToIceberg(stream, config)
      
      logger.info("Iceberg sink configured")
    } catch {
      case e: Exception =>
        logger.error(s"Error in doOutput: ${e.getMessage}", e)
    }
  }

  /**
   * Collect metrics for enrichment processing
   */
  override def collectMetrics(stream: DataStream[EnricherRecord], metrics: StreamMetrics)
                             (implicit env: StreamExecutionEnvironment, config: Map[String, String]): StreamMetrics = {
    // Add validation metrics
    metrics.addBusinessMetric("total_records_processed", totalRecordsCounter.get())
    metrics.addBusinessMetric("valid_records", validRecordsCounter.get())
    metrics.addBusinessMetric("invalid_records", invalidRecordsCounter.get())
    
    // Add validation ratio
    val total = totalRecordsCounter.get()
    val validRatio = if (total > 0) validRecordsCounter.get().toDouble / total else 0.0
    metrics.addBusinessMetric("validation_success_ratio", validRatio)
    
    metrics
  }

  /**
   * Broadcast processor that applies schema validation to incoming records
   */
  private class SchemaBroadcastProcessor extends BroadcastProcessFunction[String, JsonNode, EnricherRecord] with java.io.Serializable {
    @transient private lazy val processorLogger = LoggerFactory.getLogger(classOf[SchemaBroadcastProcessor])

    override def processElement(
                                 value: String,
                                 ctx: BroadcastProcessFunction[String, JsonNode, EnricherRecord]#ReadOnlyContext,
                                 out: Collector[EnricherRecord]
                               ): Unit = {
      // Log the raw input
      processorLogger.info(s"Received raw input: ${value.take(100)}...")
      
      // Increment total records counter
      totalRecordsCounter.incrementAndGet()

      // Get current schema from broadcast state
      val schema = ctx.getBroadcastState(SchemaMonitorUtils.schemaDescriptor).get("current")

      if (schema != null) {
        try {
          // Parse input JSON
          processorLogger.info("Attempting to parse JSON")
          val jsonNode = JsonUtils.mapper.readTree(value)
          processorLogger.info(s"JSON parsed, has id field: ${jsonNode.has("id")}")

          // Create EnricherRecord from JSON
          val record = EnricherRecord.fromJsonNode(jsonNode)
          processorLogger.info(s"Created EnricherRecord with ID: ${record.id}")

          // Validate record against schema
          processorLogger.info("Validating record against schema")
          val validatedRecord = ValidationUtils.validateRecord(record, schema)
          
          // Check if record is valid overall
          val isValid = ValidationUtils.isRecordValid(validatedRecord, schema)
          
          if (isValid) {
            validRecordsCounter.incrementAndGet()
            processorLogger.info(s"Record ${record.id} is valid")
          } else {
            invalidRecordsCounter.incrementAndGet()
            processorLogger.warn(s"Record ${record.id} failed validation")
          }

          // Output the validated record regardless of validity
          // (filtering happens downstream if configured)
          out.collect(validatedRecord)
          
        } catch {
          case e: Exception =>
            invalidRecordsCounter.incrementAndGet()
            processorLogger.error(s"Error processing record: ${e.getMessage}", e)
        }
      } else {
        processorLogger.warn("Received record but schema is not yet available")
        invalidRecordsCounter.incrementAndGet()
      }
    }

    override def processBroadcastElement(
                                          schema: JsonNode,
                                          ctx: BroadcastProcessFunction[String, JsonNode, EnricherRecord]#Context,
                                          out: Collector[EnricherRecord]
                                        ): Unit = {
      // Get current schema
      val currentSchema = ctx.getBroadcastState(SchemaMonitorUtils.schemaDescriptor).get("current")

      // Only log when schema actually changes
      if (currentSchema == null || !currentSchema.equals(schema)) {
        processorLogger.info("Schema updated in broadcast state with new content")
        
        // Log required fields from the new schema
        val requiredFields = ValidationUtils.getRequiredFields(schema)
        processorLogger.info(s"Required fields in schema: ${requiredFields.mkString(", ")}")
      }

      // Always update the broadcast state
      ctx.getBroadcastState(SchemaMonitorUtils.schemaDescriptor).put("current", schema)
    }
  }
}
