package com.insite.streams.common

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import com.insite.streams.common.metrics.StreamMetrics

/**
 * Base trait for all stream processing applications
 */
trait Stream[T] {
  /**
   * Set up and configure the stream processing pipeline
   *
   * @param env Implicit StreamExecutionEnvironment
   * @param config Implicit configuration parameters
   * @return Configured DataStream
   */
  def doLogic()(implicit env: StreamExecutionEnvironment, config: Map[String, String]): DataStream[T]

  /**
   * Sink the processed stream to destination
   *
   * @param stream DataStream to sink
   * @param env Implicit StreamExecutionEnvironment
   * @param config Implicit configuration parameters
   */
  def doOutput(stream: DataStream[T])(implicit env: StreamExecutionEnvironment, config: Map[String, String]): Unit

  /**
   * Collect metrics from the processing
   *
   * @param stream DataStream to analyze
   * @param metrics Current metrics object
   * @param env Implicit StreamExecutionEnvironment
   * @param config Implicit configuration parameters
   * @return Updated metrics
   */
  def collectMetrics(stream: DataStream[T], metrics: StreamMetrics)
                    (implicit env: StreamExecutionEnvironment, config: Map[String, String]): StreamMetrics = {
    // Default implementation returns metrics unchanged
    metrics
  }
}