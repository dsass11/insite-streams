package com.insite.streams.common.utils

import com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.slf4j.LoggerFactory
import com.insite.streams.common.utils.JsonUtils.mapper

import scala.util.{Failure, Success, Try}

/**
 * Utilities for schema monitoring and broadcasting
 */
object SchemaMonitorUtils {
  private val logger = LoggerFactory.getLogger(getClass)

  // Standard descriptor for schema broadcast state
  val schemaDescriptor = new MapStateDescriptor[String, JsonNode](
    "schema-state",
    classOf[String],
    classOf[JsonNode]
  )

  /**
   * Create a broadcast stream that monitors a schema file for changes
   *
   * @param env Flink execution environment
   * @param schemaPath Path to the schema file
   * @param refreshIntervalSeconds How often to check for schema changes (in seconds)
   * @param verboseLogging Whether to log every schema check or only changes
   * @return Broadcast stream containing schema updates
   */
  def createSchemaBroadcast(
                             env: StreamExecutionEnvironment,
                             schemaPath: String,
                             refreshIntervalSeconds: Int = 60,
                             verboseLogging: Boolean = false
                           ): BroadcastStream[JsonNode] = {

    logger.info(s"Creating schema monitor for $schemaPath with refresh interval $refreshIntervalSeconds seconds")

    // Load schema initially
    logger.info(s"Loading initial schema from $schemaPath")
    val initialSchema = loadSchema(schemaPath) match {
      case Success(schemaJson) =>
        logger.info("Initial schema loaded successfully")
        schemaJson
      case Failure(e) =>
        logger.error(s"Failed to load initial schema: ${e.getMessage}", e)
        throw e
    }

    // Create a source that periodically checks for schema updates
    val schemaSource = env.addSource(new SourceFunction[JsonNode] {
      @volatile private var isRunning = true

      override def run(ctx: SourceFunction.SourceContext[JsonNode]): Unit = {
        // Send initial schema
        ctx.collect(initialSchema)
        logger.info("Initial schema sent to broadcast")

        // Then check for updates at regular intervals
        while (isRunning) {
          try {
            Thread.sleep(refreshIntervalSeconds * 1000)

            if (verboseLogging) {
              logger.info(s"Checking for schema updates at $schemaPath")
            }

            loadSchema(schemaPath) match {
              case Success(updatedSchema) =>
                if (!updatedSchema.equals(initialSchema) || verboseLogging) {
                  logger.info("Schema file updated, broadcasting new schema")
                  ctx.collect(updatedSchema)
                }
              case Failure(e) =>
                logger.error(s"Failed to load schema for update: ${e.getMessage}", e)
            }
          } catch {
            case e: InterruptedException =>
              logger.info(s"Schema monitor interrupted: ${e.getMessage}")
              isRunning = false
            case e: Exception =>
              logger.error(s"Error in schema update check: ${e.getMessage}", e)
          }
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    }).name("Schema-Monitor-Source")

    // Broadcast the schema stream
    schemaSource.broadcast(schemaDescriptor)
  }

  /**
   * Load schema from file
   *
   * @param schemaPath Path to schema file
   * @return Loaded schema as JsonNode
   */
  def loadSchema(schemaPath: String): Try[JsonNode] = {
    Try {
      import java.nio.file.{Files, Paths}
      val content = new String(Files.readAllBytes(Paths.get(schemaPath)))
      mapper.readTree(content)
    }
  }
}