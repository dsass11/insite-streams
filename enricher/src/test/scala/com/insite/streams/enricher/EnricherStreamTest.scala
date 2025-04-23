package com.insite.streams.enricher

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

// Test mapper for serialization
object EnricherTestMapper extends Serializable {
  @transient lazy val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
}

class EnricherStreamTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  "EnricherStream" should "validate fields correctly" in {
    // Clear previous results
    EnricherTestSink.values.clear()
    
    // Set up Flink environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    
    // Load schema
    val schemaUrl = Option(getClass.getClassLoader.getResource("test-schema.json"))
      .getOrElse(throw new RuntimeException("test-schema.json not found in test resources"))
    val schemaPath = schemaUrl.getPath
    
    // Read schema content
    val schemaContent = Source.fromFile(schemaPath).mkString
    val schema = EnricherTestMapper.mapper.readTree(schemaContent)
    
    // Sample test input
    val testEvent =
      """
      {
        "id": "trail-123",
        "project_name": "Test Trail Project",
        "temperature": 25.5,
        "location": "Test Mountain",
        "humidity": 62.3,
        "reading_date": "2025-04-22",
        "notes": "Test reading"
      }
      """
    
    // Parse JSON and create EnricherRecord
    val source = env.fromElements(testEvent)
    
    // Create a record and validate it
    val records = source.map(new MapFunction[String, EnricherRecord] with Serializable {
      override def map(value: String): EnricherRecord = {
        val jsonNode = EnricherTestMapper.mapper.readTree(value)
        EnricherRecord.fromJsonNode(jsonNode)
      }
    })
    
    // Apply validation
    val validated = records.map(new MapFunction[EnricherRecord, EnricherRecord] with Serializable {
      override def map(record: EnricherRecord): EnricherRecord = {
        ValidationUtils.validateRecord(record, schema)
      }
    })
    
    // Collect results
    validated.addSink(new EnricherTestSink())
    
    // Execute the job
    env.execute("Test Enricher Validation")
    
    // Validate results
    EnricherTestSink.values should have size 1
    val result = EnricherTestSink.values.head
    
    result.id shouldBe "trail-123"
    
    // Check validation status
    result.validationStatus should contain key "project_name"
    result.validationStatus("project_name") shouldBe true
    result.validationStatus("temperature") shouldBe true
    result.validationStatus("location") shouldBe true
    result.validationStatus("humidity") shouldBe true
    result.validationStatus("reading_date") shouldBe true
    
    // Check partition date is derived
    result.fields should contain key "date_prt"
  }
}

// Sink for collecting test results
class EnricherTestSink extends SinkFunction[EnricherRecord] {
  override def invoke(value: EnricherRecord, context: SinkFunction.Context): Unit = {
    EnricherTestSink.values.synchronized {
      EnricherTestSink.values.append(value)
    }
  }
}

object EnricherTestSink {
  val values: ArrayBuffer[EnricherRecord] = new ArrayBuffer[EnricherRecord]()
}
