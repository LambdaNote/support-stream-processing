package org.apache.beam.sp_handson.sec02.p01;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class TemperatureLoggingPipeline {
  public static void main(String[] args) {
    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).withValidation().create());

    PCollection<KV<Long, String>> kafkaInput = p.apply(
        KafkaIO.<Long, String>read()
            .withBootstrapServers("localhost:9092")
            .withTopic("weather-kushiro")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withoutMetadata() // KafkaRecord -> KV
    );

    kafkaInput.apply(KafkaIO.<Long, String>write()
        .withBootstrapServers("localhost:9092")
        .withTopic("beam-out")
        .withKeySerializer(LongSerializer.class)
        .withValueSerializer(StringSerializer.class));

    p.run();
  }
}
