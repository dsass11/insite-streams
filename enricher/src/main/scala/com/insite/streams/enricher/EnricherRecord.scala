package com.insite.streams.enricher

import com.fasterxml.jackson.databind.JsonNode
import java.time.{LocalDate, ZonedDateTime}
import java.time.format.DateTimeFormatter

/**
 * Represents an enriched record with validation status
 *
 * @param id Unique identifier for the record
 * @param fields Map of field names to values
 * @param validationStatus Validation results for fields
 * @param processingTime Processing timestamp
 */
case class EnricherRecord(
  id: String,
  fields: Map[String, Any] = Map.empty,
  validationStatus: Map[String, Boolean] = Map.empty,
  processingTime: String = ZonedDateTime.now().toString
) {
  /**
   * Get a field value with type conversion
   *
   * @param name Field name
   * @tparam T Expected type
   * @return Typed field value or None if not found or wrong type
   */
  def getField[T](name: String): Option[T] = {
    fields.get(name).flatMap {
      case value: T => Some(value)
      case _ => None
    }
  }
  
  /**
   * Add or update a field
   *
   * @param name Field name
   * @param value Field value
   * @return Updated record
   */
  def withField(name: String, value: Any): EnricherRecord = {
    this.copy(fields = fields + (name -> value))
  }
  
  /**
   * Add validation status for a field
   *
   * @param name Field name
   * @param valid Whether the field is valid
   * @return Updated record
   */
  def withValidation(name: String, valid: Boolean): EnricherRecord = {
    this.copy(validationStatus = validationStatus + (name -> valid))
  }
  
  /**
   * Get partition value from processing time
   *
   * @return Partition date string in format YYYY-MM-DD
   */
  def getPartitionDate: String = {
    try {
      val dateTime = ZonedDateTime.parse(processingTime)
      dateTime.toLocalDate.toString // Returns in format YYYY-MM-DD
    } catch {
      case e: Exception => 
        // Default to today if processing time is invalid
        LocalDate.now().toString
    }
  }
  
  /**
   * Get valid fields only (fields that passed validation)
   *
   * @return Map with only valid fields
   */
  def getValidFields: Map[String, Any] = {
    fields.filter { case (fieldName, _) =>
      validationStatus.getOrElse(fieldName, false)
    }
  }
  
  /**
   * Convert to Map representation with all fields, validation status,
   * and partition information
   *
   * @return Map with all record data
   */
  def toMap: Map[String, Any] = {
    Map(
      "id" -> id,
      "processing_time" -> processingTime,
      "date_prt" -> getPartitionDate
    ) ++ fields
  }
}

/**
 * Companion object for EnricherRecord
 */
object EnricherRecord {
  private val DEFAULT_DATE_FORMAT = DateTimeFormatter.ISO_ZONED_DATE_TIME
  
  /**
   * Create EnricherRecord from JsonNode
   *
   * @param jsonNode JSON representation of a record
   * @return EnricherRecord instance
   */
  def fromJsonNode(jsonNode: JsonNode): EnricherRecord = {
    // Extract ID field
    val id = Option(jsonNode.get("id"))
      .map(_.asText())
      .getOrElse(throw new IllegalArgumentException("Record must have an ID field"))

    // Extract processing time if available, otherwise use current time
    val processingTime = Option(jsonNode.get("processing_time"))
      .map(_.asText())
      .getOrElse(ZonedDateTime.now().format(DEFAULT_DATE_FORMAT))

    // Process all fields from JSON
    val fields = new collection.mutable.HashMap[String, Any]()
    
    // Include all fields except ID and processing_time
    val fieldIterator = jsonNode.fields()
    while (fieldIterator.hasNext) {
      val entry = fieldIterator.next()
      val fieldName = entry.getKey
      val fieldValue = entry.getValue

      if (fieldName != "id" && fieldName != "processing_time") {
        // Extract value based on type
        val value = if (fieldValue.isTextual) {
          fieldValue.asText()
        } else if (fieldValue.isInt) {
          fieldValue.asInt()
        } else if (fieldValue.isLong) {
          fieldValue.asLong()
        } else if (fieldValue.isDouble) {
          fieldValue.asDouble()
        } else if (fieldValue.isBoolean) {
          fieldValue.asBoolean()
        } else if (fieldValue.isNull) {
          null
        } else {
          // For complex types, just store as string
          fieldValue.toString
        }

        fields(fieldName) = value
      }
    }

    EnricherRecord(id, fields.toMap, Map.empty, processingTime)
  }
}
