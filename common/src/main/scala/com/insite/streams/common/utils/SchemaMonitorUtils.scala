package com.insite.streams.common.utils

import com.fasterxml.jackson.databind.{JsonNode}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.FileSourceSplit
import org.apache.flink.connector.file.src.reader.BulkFormat
import org.apache.flink.connector.file.src.reader.BulkFormat.{Reader, RecordIterator}
import org.apache.flink.core.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.connector.file.src.util.RecordAndPosition
import org.apache.flink.connector.file.src.util.RecordAndPosition
import org.apache.flink.connector.file.src.reader.BulkFormat.{Reader, RecordIterator}
import org.apache.flink.connector.file.src.FileSourceSplit
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition
import java.time.Duration
import org.apache.flink.connector.file.src.util.{MutableRecordAndPosition, RecordAndPosition}
import org.apache.flink.connector.file.src.FileSourceSplit
import com.insite.streams.common.utils.JsonUtils.mapper

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
   * @param refreshIntervalMinutes How often to check for schema changes (in minutes)
   * @return Broadcast stream containing schema updates
   */
  def createSchemaBroadcast(
                             env: StreamExecutionEnvironment,
                             schemaPath: String,
                             refreshIntervalMinutes: Int = 1
                           ): BroadcastStream[JsonNode] = {

    logger.info(s"Creating schema monitor for $schemaPath with refresh interval $refreshIntervalMinutes minutes")

    // Create FileSource using BulkFormat
    val schemaSource = FileSource
      .forBulkFileFormat(new SchemaMonitorUtils.SchemaJsonFormat(), new Path(schemaPath))
      .monitorContinuously(Duration.ofMinutes(refreshIntervalMinutes))
      .build()

    // Create and return broadcast stream
    env.fromSource(
      schemaSource,
      WatermarkStrategy.noWatermarks[JsonNode](),
      "Schema Monitor",
      implicitly[TypeInformation[JsonNode]]
    ).name("Schema-Broadcast-Source")
      .setParallelism(1)
      .broadcast(schemaDescriptor)
  }

  /**
   * Format reader for schema files using BulkFormat
   */
  private class SchemaJsonFormat extends BulkFormat[JsonNode, FileSourceSplit] with Serializable {
    override def createReader(
                               config: Configuration,
                               split: FileSourceSplit
                             ): Reader[JsonNode] = {
      val fs: FileSystem = split.path().getFileSystem()
      val inputStream: FSDataInputStream = fs.open(split.path())
      val content = scala.io.Source.fromInputStream(inputStream).mkString
      val jsonNode = mapper.readTree(content)

      new Reader[JsonNode] {
        override def readBatch(): RecordIterator[JsonNode] = {
          new RecordIterator[JsonNode] {
            private var delivered = false

            override def next(): RecordAndPosition[JsonNode] = {
              if (!delivered) {
                delivered = true
                // Create a record and position with offset and recordSkipCount
                new RecordAndPosition[JsonNode](jsonNode, 0L, 0L) // offset=0, recordSkipCount=0
              } else null
            }

            override def releaseBatch(): Unit = ()
          }
        }

        override def close(): Unit = inputStream.close()
      }
    }

    override def restoreReader(config: Configuration, split: FileSourceSplit): Reader[JsonNode] = {
      createReader(config, split)
    }

    override def isSplittable: Boolean = false

    override def getProducedType: TypeInformation[JsonNode] = implicitly[TypeInformation[JsonNode]]
  }
}
