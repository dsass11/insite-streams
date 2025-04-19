package com.insite.streams.pii

import com.fasterxml.jackson.databind.JsonNode
import org.slf4j.LoggerFactory

import java.security.MessageDigest
import java.util.Base64
import scala.util.{Failure, Random, Success, Try}

/**
 * Operations for masking and transforming PII data
 */
object PIIOperations {
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Apply PII operation to a value based on field definition
   *
   * @param value Value to transform
   * @param field Field definition with PII information
   * @return Transformed value
   */
  def applyPIIOperation(value: Any, field: JsonNode): Any = {
    if (value == null) return null
    
    val operation = SchemaPIIUtils.getPIIOperation(field)
    val fieldType = SchemaPIIUtils.getFieldType(field)
    
    operation match {
      case "mask" => 
        if (fieldType == "string") {
          maskString(value.toString, SchemaPIIUtils.getMaskFormat(field))
        } else {
          value
        }
        
      case "hash" => 
        hashValue(value.toString)
        
      case "redact" => 
        if (fieldType == "string") "[REDACTED]" else null
        
      case "generalize" =>
        generalizeValue(value, SchemaPIIUtils.getFieldRanges(field))
        
      case "randomize" =>
        randomizeValue(value, fieldType)
        
      case _ =>
        logger.warn(s"Unknown PII operation: $operation, using mask as default")
        if (fieldType == "string") {
          maskString(value.toString, null)
        } else {
          value
        }
    }
  }
  
  /**
   * Mask a string value
   *
   * @param value String to mask
   * @param format Optional format pattern
   * @return Masked string
   */
  def maskString(value: String, format: String): String = {
    if (value == null || value.isEmpty) return value
    
    // If format is provided, use it
    if (format != null && !format.isEmpty) {
      applyMaskFormat(value, format)
    } else {
      // Default masking: keep first and last chars, mask the rest
      val length = value.length
      if (length <= 2) {
        "*" * length
      } else {
        val firstChar = value.charAt(0)
        val lastChar = value.charAt(length - 1)
        firstChar + ("*" * (length - 2)) + lastChar
      }
    }
  }
  
  /**
   * Apply a specific mask format to a string
   * Format uses X for keep character, * for mask
   *
   * @param value String to mask
   * @param format Format with X for visible chars
   * @return Formatted string
   */
  private def applyMaskFormat(value: String, format: String): String = {
    val result = new StringBuilder()
    val valueChars = value.toCharArray
    val formatChars = format.toCharArray
    
    for (i <- 0 until Math.min(formatChars.length, valueChars.length)) {
      formatChars(i) match {
        case 'X' | 'x' => result.append(valueChars(i))
        case _ => result.append('*')
      }
    }
    
    // If value is longer than format, mask the rest
    if (valueChars.length > formatChars.length) {
      result.append("*" * (valueChars.length - formatChars.length))
    }
    
    result.toString()
  }
  
  /**
   * Hash a value using SHA-256
   *
   * @param value Value to hash
   * @return Base64 encoded hash
   */
  def hashValue(value: String): String = {
    if (value == null || value.isEmpty) return value
    
    Try {
      val digest = MessageDigest.getInstance("SHA-256")
      val hash = digest.digest(value.getBytes("UTF-8"))
      Base64.getEncoder.encodeToString(hash)
    } match {
      case Success(hashed) => hashed
      case Failure(e) => 
        logger.error(s"Error hashing value: ${e.getMessage}")
        "[HASH_ERROR]"
    }
  }
  
  /**
   * Generalize a numeric value into ranges
   *
   * @param value Value to generalize
   * @param ranges List of range boundaries
   * @return Range representation
   */
  def generalizeValue(value: Any, ranges: List[Int]): String = {
    if (value == null || ranges.isEmpty) return value.toString
    
    val numericValue = value match {
      case i: Int => i
      case l: Long => l.toInt
      case d: Double => d.toInt
      case f: Float => f.toInt
      case s: String => Try(s.toInt).getOrElse(0)
      case _ => 0
    }
    
    val sortedRanges = ranges.sorted
    for (i <- 0 until sortedRanges.length - 1) {
      if (numericValue >= sortedRanges(i) && numericValue < sortedRanges(i + 1)) {
        return s"${sortedRanges(i)}-${sortedRanges(i + 1)}"
      }
    }
    
    // Handle values larger than the last range
    if (numericValue >= sortedRanges.last) {
      s"${sortedRanges.last}+"
    } else {
      // Handle values smaller than the first range
      s"<${sortedRanges.head}"
    }
  }
  
  /**
   * Randomize a value based on its type
   *
   * @param value Value to randomize
   * @param fieldType Field type
   * @return Randomized value
   */
  def randomizeValue(value: Any, fieldType: String): Any = {
    val random = new Random()
    
    fieldType match {
      case "string" => 
        val length = value.toString.length
        val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
        (0 until length).map(_ => chars(random.nextInt(chars.length))).mkString
        
      case "integer" | "int" =>
        val originalValue = value match {
          case i: Int => i
          case l: Long => l.toInt
          case d: Double => d.toInt
          case f: Float => f.toInt
          case s: String => Try(s.toInt).getOrElse(0)
          case _ => 0
        }
        // Generate value within ±20% of original
        val variance = Math.max(1, originalValue.abs * 0.2).toInt
        originalValue + random.nextInt(variance * 2) - variance
        
      case "double" | "float" =>
        val originalValue = value match {
          case d: Double => d
          case f: Float => f.toDouble
          case i: Int => i.toDouble
          case l: Long => l.toDouble
          case s: String => Try(s.toDouble).getOrElse(0.0)
          case _ => 0.0
        }
        // Generate value within ±20% of original
        val variance = Math.max(0.1, originalValue.abs * 0.2)
        originalValue + (random.nextDouble() * variance * 2) - variance
        
      case _ => value
    }
  }
}
