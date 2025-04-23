package com.insite.streams.enricher

import com.fasterxml.jackson.databind.JsonNode
import org.slf4j.LoggerFactory

import java.time.{LocalDate, ZonedDateTime}
import java.time.format.DateTimeFormatter
import scala.util.{Success, Try, Failure}

/**
 * Utilities for validating fields against schema
 */
object ValidationUtils {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Validate record fields against schema
   *
   * @param record Record to validate
   * @param schema Schema to validate against
   * @return Record with validation status added
   */
  def validateRecord(record: EnricherRecord, schema: JsonNode): EnricherRecord = {
    var validatedRecord = record
    
    // Get all fields from schema
    val schemaFields = getFields(schema)
    
    // Validate each field in the record
    schemaFields.foreach { fieldDef =>
      val fieldName = getFieldName(fieldDef)
      val fieldType = getFieldType(fieldDef)
      val isRequired = isFieldRequired(fieldDef)
      
      if (fieldName.nonEmpty) {
        // Check if field exists
        record.fields.get(fieldName) match {
          case Some(value) =>
            // Validate field type
            val isValid = validateType(value, fieldType)
            validatedRecord = validatedRecord.withValidation(fieldName, isValid)
            
            if (!isValid) {
              logger.warn(s"Field '$fieldName' has invalid type. Expected: $fieldType, Got: ${value.getClass.getSimpleName}")
            }
            
          case None =>
            // Field is missing
            if (isRequired) {
              logger.warn(s"Required field '$fieldName' is missing")
              validatedRecord = validatedRecord.withValidation(fieldName, false)
            } else {
              // Not required, so mark as valid even though missing
              validatedRecord = validatedRecord.withValidation(fieldName, true)
            }
        }
      }
    }
    
    // Add special derived fields if needed
    val processingTime = record.processingTime
    val partitionDate = record.getPartitionDate
    
    validatedRecord = validatedRecord
      .withField("processing_time", processingTime)
      .withField("date_prt", partitionDate)
      .withValidation("processing_time", true)
      .withValidation("date_prt", true)
    
    validatedRecord
  }
  
  /**
   * Validate a value against an expected type
   *
   * @param value Value to validate
   * @param expectedType Expected type name
   * @return Whether the value matches the expected type
   */
  def validateType(value: Any, expectedType: String): Boolean = {
    if (value == null) return true
    
    expectedType.toLowerCase match {
      case "string" => 
        value.isInstanceOf[String]
        
      case "integer" | "int" => 
        value.isInstanceOf[Int] || value.isInstanceOf[Long]
        
      case "double" | "float" => 
        value.isInstanceOf[Double] || value.isInstanceOf[Float] || 
        value.isInstanceOf[Int] || value.isInstanceOf[Long]
        
      case "boolean" => 
        value.isInstanceOf[Boolean]
        
      case "date" | "datetime" => 
        value match {
          case s: String =>
            Try(ZonedDateTime.parse(s)) match {
              case Success(_) => true
              case Failure(_) => 
                Try(LocalDate.parse(s)) match {
                  case Success(_) => true
                  case Failure(_) => false
                }
            }
          case _ => false
        }
        
      case _ => 
        // For unknown types, consider valid
        logger.warn(s"Unknown field type: $expectedType")
        true
    }
  }
  
  /**
   * Get all field definitions from schema
   *
   * @param schema Schema JSON
   * @return List of field definitions
   */
  def getFields(schema: JsonNode): List[JsonNode] = {
    val fields = schema.get("fields")
    if (fields == null || !fields.isArray) {
      logger.warn("Schema does not contain a valid 'fields' array")
      List.empty
    } else {
      val result = new collection.mutable.ListBuffer[JsonNode]
      val iterator = fields.elements()
      while (iterator.hasNext) {
        result += iterator.next()
      }
      result.toList
    }
  }
  
  /**
   * Get field name from field definition
   *
   * @param field Field definition
   * @return Field name
   */
  def getFieldName(field: JsonNode): String = {
    Option(field.get("name")).map(_.asText()).getOrElse("")
  }
  
  /**
   * Get field type from field definition
   *
   * @param field Field definition
   * @return Field type
   */
  def getFieldType(field: JsonNode): String = {
    Option(field.get("type")).map(_.asText()).getOrElse("string")
  }
  
  /**
   * Check if field is required
   *
   * @param field Field definition
   * @return Whether field is required
   */
  def isFieldRequired(field: JsonNode): Boolean = {
    Option(field.get("required"))
      .map(_.asBoolean())
      .getOrElse(false)
  }
  
  /**
   * Get all required fields from schema
   *
   * @param schema Schema JSON
   * @return List of required field names
   */
  def getRequiredFields(schema: JsonNode): List[String] = {
    getFields(schema)
      .filter(isFieldRequired)
      .map(getFieldName)
  }
  
  /**
   * Check if a record is valid (all required fields valid)
   *
   * @param record Validated record
   * @param schema Schema JSON
   * @return Whether record is valid
   */
  def isRecordValid(record: EnricherRecord, schema: JsonNode): Boolean = {
    val requiredFields = getRequiredFields(schema)
    
    // Check if all required fields are valid
    requiredFields.forall(field => 
      record.validationStatus.getOrElse(field, false)
    )
  }
}
