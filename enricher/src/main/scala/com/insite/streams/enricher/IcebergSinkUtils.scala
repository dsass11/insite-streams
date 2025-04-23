package com.insite.streams.enricher

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.{HashMap => JHashMap, Map => JMap}
import scala.collection.JavaConverters._

/**
 * Utilities for writing to Iceberg tables
 */
object IcebergSinkUtils {
  private val logger = LoggerFactory.getLogger(getClass)
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  
  /**
   * Configure Iceberg sink for a stream of EnricherRecords
   *
   * @param stream Stream of validated records
   * @param env Flink execution environment
   * @param config Configuration parameters
   */
  def writeToIceberg(stream: DataStream[EnricherRecord], config: Map[String, String])
                    (implicit env: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment): Unit = {
    try {
      // Get required configuration parameters
      val catalog = config.getOrElse("iceberg-catalog", 
        throw new IllegalArgumentException("iceberg-catalog configuration parameter is required"))
      
      val database = config.getOrElse("iceberg-database", 
        throw new IllegalArgumentException("iceberg-database configuration parameter is required"))
      
      val defaultTable = s"${database}.enrichment_ice"
      val tableName = config.getOrElse("iceberg-table", defaultTable)
      
      // Extract schema and database names from table name
      val (schemaName, tableNameOnly) = tableName.split("\\.") match {
        case Array(schema, table) => (schema, table)
        case _ => throw new IllegalArgumentException(s"Invalid table name format: $tableName. Expected format: schema.table")
      }
      
      logger.info(s"Configured Iceberg sink for table $schemaName.$tableNameOnly")
      logger.info("NOTE: The actual Iceberg write implementation is omitted in this version")
      logger.info("In a full implementation, this would use Flink's Table API to write to Iceberg")
      
      // Convert EnricherRecord stream to rows
      val rowStream = stream.map(new RecordToRowMapper())
      
      // In a real implementation, this would set up the Iceberg sink
      logger.info(s"EnricherRecord stream converted to Row stream for Iceberg table: $schemaName.$tableNameOnly")
      
    } catch {
      case e: Exception =>
        logger.error(s"Error configuring Iceberg sink: ${e.getMessage}", e)
        throw e
    }
  }
  
  /**
   * Convert a map to SQL properties format
   *
   * @param props Properties map
   * @return SQL properties string
   */
  private def mapToSqlProperties(props: JMap[String, String]): String = {
    val propsList = props.asScala.map { case (key, value) =>
      s"'$key' = '$value'"
    }.mkString(", ")
    
    s"($propsList)"
  }
  
  /**
   * Mapper to convert EnricherRecord to Row
   */
  private class RecordToRowMapper extends MapFunction[EnricherRecord, Row] with java.io.Serializable {
    @transient private lazy val mapperLogger = LoggerFactory.getLogger(classOf[RecordToRowMapper])
    
    override def map(record: EnricherRecord): Row = {
      try {
        // Get all fields including the validated ones
        val allFields = record.toMap
        
        // Create a Row with dynamic fields
        val row = Row.withNames()
        
        // Add all fields to the row
        allFields.foreach { case (fieldName, fieldValue) =>
          val value = fieldValue match {
            case null => null
            case v => v
          }
          
          row.setField(fieldName, value)
        }
        
        row
      } catch {
        case e: Exception =>
          mapperLogger.error(s"Error converting record to row: ${e.getMessage}", e)
          throw e
      }
    }
  }
}
