package com.insite.streams.common.metrics

import org.slf4j.LoggerFactory

/**
 * Simple metrics reporter that outputs to console/log
 */
class ConsoleMetricsReporter extends MetricsReporter {
  private val logger = LoggerFactory.getLogger(getClass)

  override def report(metrics: StreamMetrics): Unit = {
    logger.info("==== Stream Processing Metrics ====")
    logger.info(s"Job ID: ${metrics.jobId}")
    logger.info(s"Stream: ${metrics.streamClass}")
    logger.info(s"Name: ${metrics.name}")
    logger.info(s"Status: ${metrics.status}")
    
    val duration = metrics.endTime match {
      case Some(end) => (end - metrics.startTime) / 1000.0
      case None => (System.currentTimeMillis() - metrics.startTime) / 1000.0
    }
    logger.info(f"Duration: $duration%.2f seconds")
    
    // Log error if present
    metrics.error.foreach(err => logger.error(s"Error: $err"))
    
    // Log record counts
    if (metrics.recordCounts.nonEmpty) {
      logger.info("Record Counts:")
      metrics.recordCounts.foreach { case (name, count) =>
        logger.info(f"  $name: $count")
      }
    }
    
    // Log timings
    if (metrics.timings.nonEmpty) {
      logger.info("Timings (ms):")
      metrics.timings.foreach { case (name, ms) =>
        logger.info(f"  $name: $ms")
      }
    }
    
    // Log business metrics
    if (metrics.businessMetrics.nonEmpty) {
      logger.info("Business Metrics:")
      metrics.businessMetrics.foreach { case (name, value) =>
        logger.info(f"  $name: $value")
      }
    }
    
    // Log job metrics
    if (metrics.jobMetrics.nonEmpty) {
      logger.info("Job Metrics:")
      metrics.jobMetrics.foreach { case (name, value) =>
        logger.info(f"  $name: $value")
      }
    }
    
    logger.info("==================================")
  }
}
