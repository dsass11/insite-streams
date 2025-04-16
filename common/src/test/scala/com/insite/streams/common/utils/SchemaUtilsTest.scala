package com.insite.streams.common.utils

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SchemaUtilsTest extends AnyFlatSpec with Matchers {
  
  "SchemaUtils" should "load schema from file" in {
    val schemaPath = getClass.getResource("/test-schema.json").getPath
    val schemaResult = SchemaUtils.loadSchema(schemaPath)
    
    schemaResult.isSuccess shouldBe true
    val schema = schemaResult.get
    schema.has("fields") shouldBe true
  }
  
  it should "extract fields from schema" in {
    val schemaPath = getClass.getResource("/test-schema.json").getPath
    val schema = SchemaUtils.loadSchema(schemaPath).get
    val fields = SchemaUtils.getFields(schema)
    
    fields.size shouldBe 3
    fields.exists(f => f.get("name").asText() == "id") shouldBe true
    fields.exists(f => f.get("name").asText() == "email") shouldBe true
    fields.exists(f => f.get("name").asText() == "age") shouldBe true
  }
  
  it should "identify PII fields correctly" in {
    val schemaPath = getClass.getResource("/test-schema.json").getPath
    val schema = SchemaUtils.loadSchema(schemaPath).get
    val fields = SchemaUtils.getFields(schema)
    
    val idField = fields.find(f => f.get("name").asText() == "id").get
    val emailField = fields.find(f => f.get("name").asText() == "email").get
    
    SchemaUtils.isPIIField(idField) shouldBe false
    SchemaUtils.isPIIField(emailField) shouldBe true
  }
  
  it should "get PII operation" in {
    val schemaPath = getClass.getResource("/test-schema.json").getPath
    val schema = SchemaUtils.loadSchema(schemaPath).get
    val fields = SchemaUtils.getFields(schema)
    
    val emailField = fields.find(f => f.get("name").asText() == "email").get
    val ageField = fields.find(f => f.get("name").asText() == "age").get
    
    SchemaUtils.getPIIOperation(emailField) shouldBe "mask"
    SchemaUtils.getPIIOperation(ageField) shouldBe "generalize"
  }
  
  it should "infer SQL types correctly" in {
    SchemaUtils.inferSqlType("string value") shouldBe "STRING"
    SchemaUtils.inferSqlType(42) shouldBe "INT"
    SchemaUtils.inferSqlType(42L) shouldBe "BIGINT"
    SchemaUtils.inferSqlType(3.14) shouldBe "DOUBLE"
    SchemaUtils.inferSqlType(true) shouldBe "BOOLEAN"
  }
}
