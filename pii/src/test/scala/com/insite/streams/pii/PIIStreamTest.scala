package com.insite.streams.pii

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

class PIIStreamTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  "PIIStream" should "process and mask PII fields" in {
    // Set up test collector
    TestSinkFunction.values.clear()

    // Set up Flink environment for testing
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // Get test schema path
    val schemaPath = getClass.getResource("/sample-schema.json").getPath


    // Create sample record as JSON string
    val testRecord =
      """
      {
        "id": "user-123",
        "name": "John Smith",
        "email": "john.smith@example.com",
        "ssn": "123-45-6789",
        "age": 42,
        "address": "123 Main St, Anytown, USA",
        "phoneNumber": "555-123-4567",
        "income": 75000
      }
      """

    // Create a source that produces our test record
    val testSource = env.fromElements(testRecord)

    // Create a PII processor that takes the source and applies processing
    val piiStream = new PIIStream()

    // Apply custom logic to test the record processing in isolation
    val processedStream = testSource
      .map(jsonStr => {
        val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
        val jsonNode = mapper.readTree(jsonStr)
        PIIRecord.fromJsonNode(jsonNode)
      })
      .map(record => {
        // Manually load schema and set it on PIIStream for testing
        val loadedSchema = SchemaPIIUtils.loadSchema(schemaPath).getOrElse(fail("Failed to load schema"))
        piiStream.testMaskPIIFields(record, loadedSchema)
      })

    // Add test sink to collect results
    processedStream.addSink(new TestSinkFunction[PIIRecord])

    // Execute the job
    env.execute("PII-Stream-Test")

    // Verify results
    TestSinkFunction.values.size should be(1)

    val processedRecord = TestSinkFunction.values.head

    // Check that ID remains unchanged
    processedRecord.id shouldBe "user-123"

    // Check that PII fields are masked
    processedRecord.getField[String]("email").get should include("*")
    processedRecord.getField[String]("ssn").get should not be "123-45-6789"
    processedRecord.getField[String]("address").get shouldBe "[REDACTED]"
  }
}

// Add a public testing method to PIIStream
class PIIStream {
  // Add this method for testing purposes
  def testMaskPIIFields(record: PIIRecord, testSchema: JsonNode): PIIRecord = {
    var processedRecord = record

    // Process each field in the record
    record.fields.foreach { case (fieldName, fieldValue) =>
      // Check if field is defined in schema
      SchemaPIIUtils.getFieldByName(testSchema, fieldName).foreach { fieldDef =>
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

// Test sink to collect results
class TestSinkFunction[T] extends SinkFunction[T] {
  override def invoke(element: T, context: SinkFunction.Context): Unit = {
    TestSinkFunction.values.append(element.asInstanceOf[PIIRecord])
  }
}

object TestSinkFunction {
  val values = new ArrayBuffer[PIIRecord]()
}