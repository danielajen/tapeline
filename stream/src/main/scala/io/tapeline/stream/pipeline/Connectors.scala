package io.tapeline.stream.pipeline

import java.nio.charset.StandardCharsets

import io.tapeline.stream.config.StreamConfig
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.formats.avro.registry.confluent.{
  ConfluentRegistryAvroDeserializationSchema,
  ConfluentRegistryAvroSerializationSchema
}
import org.apache.kafka.clients.producer.ProducerRecord

/** Kafka sources and sinks, framed with Confluent Avro.
  *
  * Both directions speak the same 5-byte wire format the Go ingestion tier
  * writes, so a schema registered by the producer is resolvable by every
  * consumer without any build-time coupling between the two languages.
  */
object Connectors {

  /** Subject naming must match the producer's. TopicNameStrategy is the
    * registry default and the Go tier's `schema.SubjectForTopic`.
    */
  def subjectFor(topic: String): String = s"$topic-value"

  /** A source reading Confluent-framed Avro as GenericRecord.
    *
    * `readerSchema` is this job's view of the data. The registry supplies the
    * writer schema by id and Avro resolves the two, which is what lets a job
    * pinned to trade.v1 keep running against a producer that has moved to
    * v2 — the property proven in the Go tier's evolution test.
    */
  def avroSource(
      cfg: StreamConfig,
      topic: String,
      groupId: String,
      readerSchema: Schema,
      startFromEarliest: Boolean = false
  ): KafkaSource[GenericRecord] = {

    val offsets =
      if (startFromEarliest) OffsetsInitializer.earliest()
      // Committed offsets with an earliest fallback: a restart resumes where
      // the job stopped, and a brand new consumer group reads the retained
      // history rather than silently skipping it.
      else OffsetsInitializer.committedOffsets(
        org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST
      )

    KafkaSource
      .builder[GenericRecord]()
      .setBootstrapServers(cfg.kafkaBrokers)
      .setTopics(topic)
      .setGroupId(groupId)
      .setStartingOffsets(offsets)
      .setValueOnlyDeserializer(
        ConfluentRegistryAvroDeserializationSchema.forGeneric(readerSchema, cfg.schemaRegistryUrl)
      )
      .build()
  }

  /** An exactly-once Kafka sink for a stream of `T`.
    *
    * EXACTLY_ONCE here means Flink's two-phase commit: records are written
    * inside a Kafka transaction that commits only when the checkpoint that
    * produced them completes. The costs are real and worth stating plainly —
    * end-to-end latency becomes a function of the checkpoint interval, and
    * consumers must set `isolation.level=read_committed` or they will read
    * aborted records and the guarantee buys nothing.
    *
    * The transactional id prefix must be unique per job. Two jobs sharing one
    * will fence each other's transactions, and the symptom is a job that
    * simply stops committing.
    */
  def avroSink[T](
      cfg: StreamConfig,
      topic: String,
      transactionalIdPrefix: String,
      schemaJson: String,
      keyOf: T => String,
      toRecord: T => GenericRecord,
      eventTimeUsOf: T => Long
  ): KafkaSink[T] = {
    val props = new java.util.Properties()

    // Flink's Kafka sink defaults transaction.timeout.ms to one hour, and the
    // Kafka broker default for transaction.max.timeout.ms is fifteen minutes.
    // The producer therefore cannot initialise at all, and the job crash-loops
    // with "The transaction timeout is larger than the maximum value allowed
    // by the broker" — which names the two settings but not which side to
    // change.
    //
    // The rule this value has to satisfy: it must exceed the longest plausible
    // checkpoint interval plus recovery time, because a transaction stays open
    // across a checkpoint and an expired one loses data. It must also stay
    // under the broker's maximum. Ten minutes sits comfortably between a 30s
    // checkpoint interval and the 15-minute broker ceiling.
    props.setProperty("transaction.timeout.ms", "600000")

    KafkaSink
      .builder[T]()
      .setKafkaProducerConfig(props)
      .setBootstrapServers(cfg.kafkaBrokers)
      .setRecordSerializer(
        new AvroKeyedSerializer[T](
          topic = topic,
          subject = subjectFor(topic),
          schemaJson = schemaJson,
          registryUrl = cfg.schemaRegistryUrl,
          keyOf = keyOf,
          toRecord = toRecord,
          eventTimeUsOf = eventTimeUsOf
        )
      )
      .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .setTransactionalIdPrefix(transactionalIdPrefix)
      .build()
  }
}

/** Serializes `T` to a Confluent-framed Avro value with an explicit key.
  *
  * The schema is carried as JSON, not as an `org.apache.avro.Schema`: Avro's
  * Schema is not Serializable, and Flink ships every operator to the
  * TaskManagers by serializing it. Holding the parsed object here would fail
  * at submit time.
  */
final class AvroKeyedSerializer[T](
    topic: String,
    subject: String,
    schemaJson: String,
    registryUrl: String,
    keyOf: T => String,
    toRecord: T => GenericRecord,
    eventTimeUsOf: T => Long
) extends KafkaRecordSerializationSchema[T] {

  @transient private var inner: SerializationSchema[GenericRecord] = _

  override def open(
      context: SerializationSchema.InitializationContext,
      sinkContext: KafkaRecordSerializationSchema.KafkaSinkContext
  ): Unit = {
    val schema = new Schema.Parser().parse(schemaJson)
    val delegate = ConfluentRegistryAvroSerializationSchema.forGeneric(subject, schema, registryUrl)
    delegate.open(context)
    inner = delegate
  }

  override def serialize(
      element: T,
      context: KafkaRecordSerializationSchema.KafkaSinkContext,
      timestamp: java.lang.Long
  ): ProducerRecord[Array[Byte], Array[Byte]] =
    new ProducerRecord[Array[Byte], Array[Byte]](
      topic,
      null, // let the partitioner hash the key
      java.lang.Long.valueOf(eventTimeUsOf(element) / 1000L),
      keyOf(element).getBytes(StandardCharsets.UTF_8),
      inner.serialize(toRecord(element))
    )
}
