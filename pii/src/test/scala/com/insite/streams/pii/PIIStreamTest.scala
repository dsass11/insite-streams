package com.insite.streams.pii

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

// Avoid serialization issues by externalizing ObjectMapper
object TestMapper extends Serializable {
  @transient lazy val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
}

class PIIStreamTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  "PIIStream" should "process and mask PII fields correctly" in {
    // Clear previous results
    TestSinkFunction.values.clear()

    // Set up Flink environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // Load schema
    val schemaUrl = Option(getClass.getClassLoader.getResource("sample-schema.json"))
      .getOrElse(throw new RuntimeException("sample-schema.json not found in test resources"))

    val schemaPath = schemaUrl.getPath
    val schema = SchemaPIIUtils.loadSchema(schemaPath).getOrElse(fail("Schema not loaded"))
    val piiStream = new PIIStream()

    // Sample test input
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

    val source = env.fromElements(testRecord)

    // Define safe JSON parser map function
    val parsed = source.map(new MapFunction[String, PIIRecord] with Serializable {
      override def map(json: String): PIIRecord = {
        val jsonNode = TestMapper.mapper.readTree(json)
        PIIRecord.fromJsonNode(jsonNode)
      }
    })

    // Apply masking using a serializable map function
    val masked = parsed.map(new MapFunction[PIIRecord, PIIRecord] with Serializable {
      override def map(record: PIIRecord): PIIRecord = SchemaPIIUtils.maskPIIFields(record, schema)
    })

    // Add test sink
    masked.addSink(new TestSinkFunction[PIIRecord])

    // Run test job
    env.execute("PII Stream Masking Test")

    // Validate output
    TestSinkFunction.values should have size 1
    val result = TestSinkFunction.values.head

    result.id shouldBe "user-123"
    result.getField[String]("email").get should include("*")
    result.getField[String]("ssn").get should not be "123-45-6789"
    result.getField[String]("address").get shouldBe "[REDACTED]"
  }
}

// Add test sink to collect output
class TestSinkFunction[T] extends SinkFunction[T] {
  override def invoke(value: T, context: SinkFunction.Context): Unit = {
    TestSinkFunction.values.append(value.asInstanceOf[PIIRecord])
  }
}

object TestSinkFunction {
  val values: ArrayBuffer[PIIRecord] = new ArrayBuffer[PIIRecord]()
}
