package com.insite.streams.pii

import com.fasterxml.jackson.databind.JsonNode

/**
 * Represents a record with potential PII fields
 *
 * @param id Unique identifier for the record
 * @param fields Map of field names to values
 * @param dynamicProps Additional properties not in the schema
 */
case class PIIRecord(
  id: String,
  fields: Map[String, Any] = Map.empty,
  dynamicProps: Map[String, Any] = Map.empty
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
  def withField(name: String, value: Any): PIIRecord = {
    this.copy(fields = fields + (name -> value))
  }
  
  /**
   * Add or update a dynamic property
   *
   * @param name Property name
   * @param value Property value
   * @return Updated record
   */
  def withDynamicProp(name: String, value: Any): PIIRecord = {
    this.copy(dynamicProps = dynamicProps + (name -> value))
  }
  
  /**
   * Convert to Map representation with all fields and properties
   *
   * @return Map with all record data
   */
  def toMap: Map[String, Any] = {
    Map("id" -> id) ++ fields ++ dynamicProps
  }
}

/**
 * Companion object for PIIRecord
 */
object PIIRecord {
  /**
   * Create PIIRecord from JsonNode
   *
   * @param jsonNode JSON representation of a record
   * @param schema Optional schema for field validation
   * @return PIIRecord instance
   */
  def fromJsonNode(jsonNode: JsonNode, schema: Option[JsonNode] = None): PIIRecord = {
    // Extract ID field
    val id = Option(jsonNode.get("id"))
      .map(_.asText())
      .getOrElse(throw new IllegalArgumentException("Record must have an ID field"))
    
    // Process all fields from JSON
    val fields = new collection.mutable.HashMap[String, Any]()
    val dynamicProps = new collection.mutable.HashMap[String, Any]()
    
    val fieldIterator = jsonNode.fields()
    while (fieldIterator.hasNext) {
      val entry = fieldIterator.next()
      val fieldName = entry.getKey
      val fieldValue = entry.getValue
      
      if (fieldName != "id") {
        // Check if field exists in schema (if provided)
        val isSchemaField = schema
          .flatMap(s => Option(SchemaPIIUtils.getFieldByName(s, fieldName)))
          .isDefined
        
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
        
        // Add to appropriate map
        if (isSchemaField || schema.isEmpty) {
          fields(fieldName) = value
        } else {
          dynamicProps(fieldName) = value
        }
      }
    }
    
    PIIRecord(id, fields.toMap, dynamicProps.toMap)
  }
}
