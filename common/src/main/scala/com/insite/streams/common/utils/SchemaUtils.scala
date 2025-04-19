package com.insite.streams.common.utils

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.LoggerFactory

import java.io.File
import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import com.insite.streams.common.utils.JsonUtils.mapper
/**
 * Utilities for schema handling
 */
object SchemaUtils {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Load schema definition from file
   *
   * @param schemaPath Path to schema file
   * @return Parsed schema as JsonNode
   */
  def loadSchema(schemaPath: String): Try[JsonNode] = {
    Try {
      val file = new File(schemaPath)
      if (!file.exists()) {
        throw new IllegalArgumentException(s"Schema file not found: $schemaPath")
      }
      mapper.readTree(file)
    }
  }
  
  /**
   * Get field definitions from schema
   *
   * @param schema Loaded schema
   * @return List of field definitions
   */
  def getFields(schema: JsonNode): List[JsonNode] = {
    val fields = schema.get("fields")
    if (fields == null || !fields.isArray) {
      logger.warn("Schema does not contain a valid 'fields' array")
      List.empty
    } else {
      val result = new mutable.ListBuffer[JsonNode]
      val iterator = fields.elements()
      while (iterator.hasNext) {
        result += iterator.next()
      }
      result.toList
    }
  }
  
  /**
   * Check if a field is marked as PII in the schema
   *
   * @param field Field definition
   * @return True if field is PII
   */
  def isPIIField(field: JsonNode): Boolean = {
    val isPII = field.get("isPII")
    isPII != null && isPII.asBoolean()
  }
  
  /**
   * Get PII operation for a field
   *
   * @param field Field definition
   * @return PII operation name
   */
  def getPIIOperation(field: JsonNode): String = {
    val operation = field.get("piiOperation")
    if (operation != null) operation.asText() else "mask"
  }
  
  /**
   * Infer SQL data type from a value
   */
  def inferSqlType(value: Any): String = value match {
    case _: String => "STRING"
    case _: Int | _: java.lang.Integer => "INT"
    case _: Long | _: java.lang.Long => "BIGINT"
    case _: Double | _: java.lang.Double => "DOUBLE"
    case _: Float | _: java.lang.Float => "FLOAT"
    case _: Boolean | _: java.lang.Boolean => "BOOLEAN"
    case _: java.sql.Date => "DATE"
    case _: java.sql.Timestamp => "TIMESTAMP(3)"
    case _ => "STRING" // Default to string for unknown types
  }
}
