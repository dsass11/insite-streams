package com.insite.streams.common.metrics

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StreamMetricsTest extends AnyFlatSpec with Matchers {
  
  "StreamMetrics" should "be created with required parameters" in {
    val metrics = StreamMetrics(
      jobId = "test-job-1",
      streamClass = "TestStream",
      name = "Test Job",
      startTime = 1000L
    )
    
    metrics.jobId shouldBe "test-job-1"
    metrics.streamClass shouldBe "TestStream"
    metrics.name shouldBe "Test Job"
    metrics.startTime shouldBe 1000L
    metrics.status shouldBe "RUNNING"
    metrics.error shouldBe None
    metrics.recordCounts shouldBe empty
    metrics.timings shouldBe empty
    metrics.businessMetrics shouldBe empty
    metrics.jobMetrics shouldBe empty
  }
  
  it should "update status correctly" in {
    val metrics = StreamMetrics("job-1", "TestStream", "Test", 1000L)
    val updated = metrics.withStatus("COMPLETED")
    
    updated.status shouldBe "COMPLETED"
    updated.jobId shouldBe metrics.jobId  // Other fields should remain unchanged
  }
  
  it should "add record counts" in {
    val metrics = StreamMetrics("job-1", "TestStream", "Test", 1000L)
    val withCounts = metrics
      .addRecordCount("processed", 100)
      .addRecordCount("filtered", 25)
    
    withCounts.recordCounts should contain allOf(
      "processed" -> 100L,
      "filtered" -> 25L
    )
  }
  
  it should "add business metrics of different types" in {
    val metrics = StreamMetrics("job-1", "TestStream", "Test", 1000L)
    val withMetrics = metrics
      .addBusinessMetric("string-metric", "string-value")
      .addBusinessMetric("int-metric", 42)
      .addBusinessMetric("double-metric", 3.14)
    
    withMetrics.businessMetrics should contain allOf(
      "string-metric" -> "string-value",
      "int-metric" -> 42,
      "double-metric" -> 3.14
    )
  }
}
