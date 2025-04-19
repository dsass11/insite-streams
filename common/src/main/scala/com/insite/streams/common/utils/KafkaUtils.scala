package com.insite.streams.common.utils

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaSink, KafkaRecordSerializationSchema}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

/**
 * Utilities for Kafka source and sink creation
 */
object KafkaUtils {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create a Kafka source for reading string messages
   *
   * @param topic Kafka topic to read from
   * @param bootstrapServers Kafka bootstrap servers
   * @param groupId Consumer group ID
   * @param config Additional configuration parameters
   * @return Configured Kafka source
   */
  def createStringSource(topic: String,
                         bootstrapServers: String = "localhost:9092",
                         groupId: String = "default-group",
                         config: Map[String, String] = Map.empty): KafkaSource[String] = {

    logger.info(s"Creating Kafka source for topic: $topic, bootstrap servers: $bootstrapServers, group ID: $groupId")

    val sourceBuilder = KafkaSource.builder[String]()
      .setBootstrapServers(bootstrapServers)
      .setTopics(topic)
      .setGroupId(groupId)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())

    // Apply any additional configuration
    config.foreach { case (key, value) =>
      sourceBuilder.setProperty(key, value)
    }

    sourceBuilder.build()
  }

  /**
   * Read from a Kafka topic and convert to a DataStream
   *
   * @param topic Kafka topic to read from
   * @param env Flink execution environment
   * @param bootstrapServers Kafka bootstrap servers
   * @param groupId Consumer group ID
   * @param config Additional configuration parameters
   * @return DataStream of strings from Kafka
   */
  def readStringStream(topic: String,
                       env: StreamExecutionEnvironment,
                       bootstrapServers: String = "localhost:9092",
                       groupId: String = "default-group",
                       config: Map[String, String] = Map.empty): DataStream[String] = {

    val source = createStringSource(topic, bootstrapServers, groupId, config)

    env.fromSource(source, WatermarkStrategy.noWatermarks[String](), s"Kafka Source: $topic")
  }

  /**
   * Create a Kafka sink for writing string messages
   *
   * @param topic Kafka topic to write to
   * @param bootstrapServers Kafka bootstrap servers
   * @param config Additional configuration parameters
   * @return Configured Kafka sink
   */
  def createStringSink(topic: String,
                       bootstrapServers: String = "localhost:9092",
                       config: Map[String, String] = Map.empty): KafkaSink[String] = {

    logger.info(s"Creating Kafka sink for topic: $topic, bootstrap servers: $bootstrapServers")

    // Create serialization schema
    val serializationSchema = KafkaRecordSerializationSchema.builder[String]()
      .setTopic(topic)
      .setValueSerializationSchema(new SimpleStringSchema())
      .build()

    // Create sink builder
    val sinkBuilder = KafkaSink.builder[String]()
      .setBootstrapServers(bootstrapServers)
      .setRecordSerializer(serializationSchema)
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)

    // Apply any additional configuration
    config.foreach { case (key, value) =>
      sinkBuilder.setProperty(key, value)
    }

    sinkBuilder.build()
  }

  /**
   * Write a DataStream to Kafka
   *
   * @param stream DataStream to write
   * @param topic Kafka topic to write to
   * @param bootstrapServers Kafka bootstrap servers
   * @param config Additional configuration parameters
   */
  def writeStringStream(stream: DataStream[String],
                        topic: String,
                        bootstrapServers: String = "localhost:9092",
                        config: Map[String, String] = Map.empty): Unit = {

    val sink = createStringSink(topic, bootstrapServers, config)

    stream.sinkTo(sink)
  }
}