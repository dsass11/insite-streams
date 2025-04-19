package com.insite.streams.common

import com.insite.streams.common.metrics.{StreamMetrics, MetricsReporter, ConsoleMetricsReporter}
import com.insite.streams.common.utils.FlinkUtils
import org.apache.flink.api.common.JobID
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

import java.util.UUID

/**
 * Generic runner for Stream implementations
 */
object StreamRunner {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.error("Usage: StreamRunner <stream-class-name> [param1=value1 param2=value2 ...]")
      System.exit(1)
    }

    val streamClassName = args(0)

    // Parse configuration from command line
    implicit val config: Map[String, String] = parseConfig(args.drop(1))

    // Create a unique job ID
    val jobId = config.getOrElse("job-id", UUID.randomUUID().toString)

    logger.info(s"Running Stream: $streamClassName (Job ID: $jobId)")
    logger.info(s"Configuration: $config")

    // Initialize metrics
    var metrics = StreamMetrics(
      jobId = jobId,
      streamClass = streamClassName,
      name = config.getOrElse("metrics-name", "Stream Job"),
      startTime = System.currentTimeMillis()
    )

    // Create metrics reporter
    val reporter = createReporter(config)

    // Create Flink execution environment
    implicit val env = FlinkUtils.createExecutionEnvironment(config)

    try {
      // Use a type-safe method to handle the dynamic instantiation and processing
      runStream(streamClassName, metrics, reporter)

    } catch {
      case e: Exception =>
        // Update metrics with error information
        metrics = metrics
          .withStatus("FAILED")
          .withError(s"${e.getClass.getName}: ${e.getMessage}")
          .withEndTime(System.currentTimeMillis())

        logger.error(s"Error executing Stream $streamClassName: ${e.getMessage}", e)
        System.exit(1)

        // Report metrics
        reporter.report(metrics)
    }
  }

  /**
   * Type-safe method to run a Stream implementation
   */
  private def runStream[T](streamClassName: String, metrics: StreamMetrics, reporter: MetricsReporter)
                          (implicit env: StreamExecutionEnvironment, config: Map[String, String]): Unit = {
    try {
      // Dynamically load and instantiate the Stream class
      val streamClass = Class.forName(streamClassName)
      val stream = streamClass.getDeclaredConstructor().newInstance().asInstanceOf[Stream[T]]

      // Execute the stream logic
      val processedStream = stream.doLogic()

      // Send to output
      stream.doOutput(processedStream)

      // Collect metrics
      val updatedMetrics = stream.collectMetrics(processedStream, metrics)

      // Execute the job
      val result = env.execute(s"$streamClassName - ${metrics.jobId}")

      // Update metrics with job details
      val finalMetrics = updatedMetrics
        .withStatus("SUCCESS")
        .withEndTime(System.currentTimeMillis())
        .addJobMetrics("jobId", result.getJobID.toString)
        .addJobMetrics("netRuntime", result.getNetRuntime)

      // Report metrics
      reporter.report(finalMetrics)

      logger.info(s"Successfully executed Stream: $streamClassName")
    } catch {
      case e: Exception =>
        // Update metrics with error information
        val errorMetrics = metrics
          .withStatus("FAILED")
          .withError(s"${e.getClass.getName}: ${e.getMessage}")
          .withEndTime(System.currentTimeMillis())

        // Report metrics
        reporter.report(errorMetrics)

        logger.error(s"Error executing Stream $streamClassName: ${e.getMessage}", e)
        throw e
    }
  }

  /**
   * Parse command line arguments into configuration
   */
  private def parseConfig(args: Array[String]): Map[String, String] = {
    args.flatMap { arg =>
      val parts = arg.split("=", 2)
      if (parts.length == 2) {
        Some(parts(0) -> parts(1))
      } else {
        logger.warn(s"Ignoring malformed parameter: $arg")
        None
      }
    }.toMap
  }

  /**
   * Create metrics reporter based on configuration
   */
  private def createReporter(config: Map[String, String]): MetricsReporter = {
    val reporterType = config.getOrElse("metrics-reporter", "console")

    reporterType match {
      case "console" => new ConsoleMetricsReporter()
      // Add additional reporters as needed
      case _ =>
        logger.warn(s"Unknown metrics reporter type: $reporterType, defaulting to console")
        new ConsoleMetricsReporter()
    }
  }
}