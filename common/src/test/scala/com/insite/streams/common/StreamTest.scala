package com.insite.streams.common

import com.insite.streams.common.metrics.StreamMetrics
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util


class StreamTest extends AnyFlatSpec with Matchers {

  "Stream trait" should "allow implementation of required methods" in {
    // Create a simple test implementation of the Stream trait with String type parameter
    val testStream = new Stream[String] {
      override def doLogic()(implicit env: StreamExecutionEnvironment, config: Map[String, String]): DataStream[String] = {
        // Create a simple stream from a collection
        val collection = new util.ArrayList[String]
        collection.add("test1")
        collection.add("test2")

        env.fromCollection(collection)
      }

      override def doOutput(stream: DataStream[String])(implicit env: StreamExecutionEnvironment, config: Map[String, String]): Unit = {
        // In test, we just add a simple print sink
        stream.print()
      }

      override def collectMetrics(stream: DataStream[String], metrics: StreamMetrics)
                                 (implicit env: StreamExecutionEnvironment, config: Map[String, String]): StreamMetrics = {
        // Add a test metric
        metrics.addBusinessMetric("test_metric", "test_value")
      }
    }

    // Set up environment and config for testing
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    implicit val config: Map[String, String] = Map("test" -> "value")

    // Test the implementation
    val stream = testStream.doLogic()
    testStream.doOutput(stream)

    val metrics = StreamMetrics("test-job", "TestStream", "Test", System.currentTimeMillis())
    val updatedMetrics = testStream.collectMetrics(stream, metrics)

    updatedMetrics.businessMetrics should contain("test_metric" -> "test_value")
  }
}