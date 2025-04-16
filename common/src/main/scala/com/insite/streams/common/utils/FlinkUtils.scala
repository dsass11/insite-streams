package com.insite.streams.common.utils

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.CheckpointingMode
import org.slf4j.LoggerFactory

/**
 * Utility functions for Flink environments and operations
 */
object FlinkUtils {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create a configured StreamExecutionEnvironment based on configuration
   *
   * @param config Configuration parameters
   * @return Configured StreamExecutionEnvironment
   */
  def createExecutionEnvironment(config: Map[String, String]): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    // Configure parallelism
    val parallelism = config.get("parallelism").map(_.toInt).getOrElse(1)
    env.setParallelism(parallelism)
    
    // Configure checkpointing
    val checkpointInterval = config.get("checkpoint.interval").map(_.toLong).getOrElse(10000L)
    if (checkpointInterval > 0) {
      env.enableCheckpointing(checkpointInterval)
      
      // Configure checkpoint mode
      val checkpointMode = config.getOrElse("checkpoint.mode", "at_least_once")
      checkpointMode match {
        case "exactly_once" => env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        case _ => env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
      }
      
      // Configure checkpoint timeout
      val checkpointTimeout = config.get("checkpoint.timeout").map(_.toLong).getOrElse(60000L)
      env.getCheckpointConfig.setCheckpointTimeout(checkpointTimeout)
      
      // Configure checkpoint directory
      config.get("checkpoint.dir").foreach { dir =>
        val conf = new Configuration()
        conf.setString("state.checkpoints.dir", dir)
        // Note: This assumes you're using Flink > 1.12 which supports configure()
        // For older versions, you may need to use alternative approaches
        env.configure(conf)
      }
    }
    
    // Configure execution mode
    val executionMode = config.getOrElse("execution.mode", "streaming")
    executionMode match {
      case "batch" => env.setRuntimeMode(RuntimeExecutionMode.BATCH)
      case "automatic" => env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
      case _ => env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    }
    
    env
  }
}
