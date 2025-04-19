package com.insite.streams.pii

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.LoggerFactory

import java.io.File
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
 * Utilities for handling PII-specific schema operations
 */
object SchemaPIIUtils {
  private val logger = LoggerFactory.getLogger(getClass)
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  
  /**
   * Load PII schema definition from file
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
      val iterator = fields.elements()
      val result = new mutable.ListBuffer[JsonNode]
      while (iterator.hasNext) {
        result += iterator.next()
      }
      result.toList
    }
  }
  
  /**
   * Get a field definition by name
   *
   * @param schema Loaded schema
   * @param fieldName Field name to find
   * @return Optional field definition
   */
  def getFieldByName(schema: JsonNode, fieldName: String): Option[JsonNode] = {
    getFields(schema).find(f => 
      Option(f.get("name")).exists(_.asText() == fieldName)
    )
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
   * Get all PII fields from a schema
   *
   * @param schema Loaded schema
   * @return List of PII field definitions
   */
  def getAllPIIFields(schema: JsonNode): List[JsonNode] = {
    getFields(schema).filter(isPIIField)
  }
  
  /**
   * Get field type from definition
   *
   * @param field Field definition
   * @return Field type as string
   */
  def getFieldType(field: JsonNode): String = {
    val typeNode = field.get("type")
    if (typeNode != null) typeNode.asText() else "string"
  }
  
  /**
   * Get field size if defined
   *
   * @param field Field definition
   * @return Optional field size
   */
  def getFieldSize(field: JsonNode): Option[Int] = {
    val sizeNode = field.get("size")
    if (sizeNode != null && sizeNode.isInt) Some(sizeNode.asInt()) else None
  }
  
  /**
   * Get field ranges for generalization (if defined)
   *
   * @param field Field definition
   * @return List of range boundaries or empty list if not defined
   */
  def getFieldRanges(field: JsonNode): List[Int] = {
    val rangesNode = field.get("ranges")
    if (rangesNode != null && rangesNode.isArray) {
      val iterator = rangesNode.elements()
      val result = new mutable.ListBuffer[Int]
      while (iterator.hasNext) {
        val element = iterator.next()
        if (element.isInt) {
          result += element.asInt()
        }
      }
      result.toList
    } else {
      List.empty
    }
  }
  
  /**
   * Get mask format for a field (if defined)
   *
   * @param field Field definition
   * @return Mask format or default
   */
  def getMaskFormat(field: JsonNode): String = {
    val formatNode = field.get("maskFormat")
    if (formatNode != null) formatNode.asText() else null
  }

  /**
   * Apply PII masking to all fields in a record based on schema
   */
  def maskPIIFields(record: PIIRecord, schema: JsonNode): PIIRecord = {
    var processedRecord = record

    // Process each field in the record
    record.fields.foreach { case (fieldName, fieldValue) =>
      // Check if field is defined in schema
      SchemaPIIUtils.getFieldByName(schema, fieldName).foreach { fieldDef =>
        // Check if field is PII
        if (SchemaPIIUtils.isPIIField(fieldDef)) {
          // Apply PII operation
          val maskedValue = PIIOperations.applyPIIOperation(fieldValue, fieldDef)
          processedRecord = processedRecord.withField(fieldName, maskedValue)
        }
      }
    }

    processedRecord
  }
}
