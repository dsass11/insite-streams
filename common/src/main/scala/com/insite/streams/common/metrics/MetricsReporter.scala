package com.insite.streams.common.metrics

/**
 * Interface for reporting stream metrics
 */
trait MetricsReporter {
  /**
   * Report metrics
   * 
   * @param metrics The metrics to report
   */
  def report(metrics: StreamMetrics): Unit
}
