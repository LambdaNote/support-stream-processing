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

public class TemperaturePassthroughPipeline {
  public static void main(String[] args) {
    // 空のパイプラインを作成
    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).withValidation().create());

    // 入力トランスフォームは、パイプライン(p)から .apply() で生やす
    PCollection<KV<Long, String>> kafkaInput = p.apply(
        // 入力トランスフォームを作る
        KafkaIO.<Long, String>read()
            // Kafkaのbootstrap-serverへの接続情報
            .withBootstrapServers("localhost:9092")
            // トピック
            .withTopic("weather-kushiro")

            // KafkaからconsumeするKey-Valueのシリアライズ方法を指示
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)

            // 出力PCollectionの型をKafkaRecordではなくKVとする
            .withoutMetadata());

    // kafkaInputが上記入力トランスフォームの出力PCollection
    // 入力トランスフォーム以外のトランスフォームは、上流のPCollectionから .apply() で生やす
    kafkaInput.apply(
        // 出力トランスフォームを作る
        KafkaIO.<Long, String>write()
            // Kafkaのbootstrap-serverへの接続情報
            .withBootstrapServers("localhost:9092")
            // トピック
            .withTopic("beam-out")
            // シリアライズ指示
            .withKeySerializer(LongSerializer.class)
            .withValueSerializer(StringSerializer.class));

    // パイプラインを起動
    p.run();
  }
}
