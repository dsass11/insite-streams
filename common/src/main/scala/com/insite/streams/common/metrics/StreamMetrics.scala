package com.insite.streams.common.metrics

/**
 * Metrics model for stream processing
 */
case class StreamMetrics(
  jobId: String,
  streamClass: String,
  name: String,
  startTime: Long,
  endTime: Option[Long] = None,
  status: String = "RUNNING",
  error: Option[String] = None,
  recordCounts: Map[String, Long] = Map.empty,
  timings: Map[String, Long] = Map.empty,
  businessMetrics: Map[String, Any] = Map.empty,
  jobMetrics: Map[String, Any] = Map.empty
) {
  def withEndTime(time: Long): StreamMetrics = this.copy(endTime = Some(time))
  
  def withStatus(status: String): StreamMetrics = this.copy(status = status)
  
  def withError(error: String): StreamMetrics = this.copy(error = Some(error))
  
  def addRecordCount(name: String, count: Long): StreamMetrics = 
    this.copy(recordCounts = recordCounts + (name -> count))
  
  def addTiming(name: String, timeMs: Long): StreamMetrics = 
    this.copy(timings = timings + (name -> timeMs))
  
  def addBusinessMetric[T](name: String, value: T): StreamMetrics = 
    this.copy(businessMetrics = businessMetrics + (name -> value))
    
  def addJobMetrics[T](name: String, value: T): StreamMetrics = 
    this.copy(jobMetrics = jobMetrics + (name -> value))
}
