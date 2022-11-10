package org.apache.beam.sp_handson.sec02.p02;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonParsePipeline {
  // アメダス観測のJSON文字列をWeatherクラスにマッピングするため
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static void main(String[] args) {
    // 空のパイプラインを作成
    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).withValidation().create());

    PCollection<KV<Long, String>> kafkaInput = p.apply(
        KafkaIO.<Long, String>read()
            .withBootstrapServers("localhost:9092")
            .withTopic("weather-kushiro")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withReadCommitted()
            .withoutMetadata());

    PCollection<Weather> weather = kafkaInput.apply(
        ParDo.of(new DoFn<KV<Long, String>, Weather>() { // a DoFn as an anonymous inner class instance
          @ProcessElement
          public void processElement(@Element KV<Long, String> rawWeather, OutputReceiver<Weather> out)
              throws JsonProcessingException {
            String jsonWeather = rawWeather.getValue();
            Weather weather = objectMapper.readValue(jsonWeather, Weather.class);
            out.output(weather);
          }
        }));

    PCollection<String> weatherLine = weather.apply(
        MapElements
            .into(TypeDescriptors.strings())
            .via(Weather::toLine));

    // kafkaInputが上記入力トランスフォームの出力PCollection
    // 入力トランスフォーム以外のトランスフォームは、上流のPCollectionから .apply() で生やす
    weatherLine.apply(
        // 出力トランスフォームを作る
        KafkaIO.<Void, String>write()
            // Kafkaのbootstrap-serverへの接続情報
            .withBootstrapServers("localhost:9092")
            // トピック
            .withTopic("beam-out")
            // シリアライズ指示
            .withValueSerializer(StringSerializer.class)
            .values());

    // パイプラインを起動
    p.run();
  }
}
