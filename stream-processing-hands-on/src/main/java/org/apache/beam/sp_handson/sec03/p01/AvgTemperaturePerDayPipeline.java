package org.apache.beam.sp_handson.sec03.p01;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sp_handson.sec02.p02.Weather;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AvgTemperaturePerDayPipeline {
  // アメダス観測のJSON文字列をWeatherクラスにマッピングするため
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static void main(String[] args) {
    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).withValidation().create());

    // Kafkaソース入力
    PCollection<KV<Long, String>> kafkaInput = p.apply(
        KafkaIO.<Long, String>read()
            .withBootstrapServers("localhost:9092")
            .withTopic("weather-kushiro")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withReadCommitted()
            .withoutMetadata());

    // KV<Long, String> のバリュー部分をJSONとしてパースし、Weatherクラスにマッピング
    PCollection<Weather> weather = kafkaInput.apply(
        ParDo.of(new DoFn<KV<Long, String>, Weather>() {
          @ProcessElement
          public void processElement(@Element KV<Long, String> rawWeather, OutputReceiver<Weather> out)
              throws JsonProcessingException {
            String jsonWeather = rawWeather.getValue();
            Weather weather = objectMapper.readValue(jsonWeather, Weather.class);
            out.output(weather);
          }
        }));

    // Event timeを設定しつつ、気温のみ抽出
    PCollection<Float> timestampedTemperature = weather.apply(
        ParDo.of(new DoFn<Weather, Float>() {
          @ProcessElement
          public void processElement(
              @FieldAccess("timestamp") String timestamp,
              @FieldAccess("temperatureC") float temperatureC,
              OutputReceiver<Float> out) {
            // 観測日時をパース
            Instant eventTime = Instant.parse(timestamp);
            // 観測日時をevent timeとして設定
            out.outputWithTimestamp(temperatureC, eventTime);
          }
        }));

    PCollection<Float> windowedTemperature = timestampedTemperature.apply(
        Window.<Float>into(FixedWindows.of(Duration.standardDays(1))));

    PCollection<Double> meanTemperature = windowedTemperature.apply(
        Mean.<Float>globally());

    // フォーマットして文字列化。その際window開始時刻もとる
    PCollection<String> meanTemperatureLine = meanTemperature.apply(
        ParDo.of(new DoFn<Double, String>() {
          @ProcessElement
          public void processElement(
              @Element Double mean,
              IntervalWindow window,
              OutputReceiver<String> out) {
            String line = window.start().toString() + "\tmeanTemperature(daily):" + mean;
            out.output(line);
          }
        }));

    // Kafkaシンク出力
    meanTemperatureLine.apply(
        KafkaIO.<Void, String>write()
            .withBootstrapServers("localhost:9092")
            .withTopic("beam-out")
            .withValueSerializer(StringSerializer.class)
            .values());

    p.run();
  }
}
