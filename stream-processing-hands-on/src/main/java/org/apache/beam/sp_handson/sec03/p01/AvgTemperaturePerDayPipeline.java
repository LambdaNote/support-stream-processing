package org.apache.beam.sp_handson.sec03.p01;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
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

    // Event timeを設定しつつ、過去の日時の気象情報データを許容できるようにする
    //
    // .withAllowedTimestampSkewはdeprecatedになっており
    // https://issues.apache.org/jira/browse/BEAM-644 で代替が提案されているが、
    // Beam v2.42.0点では大体は使えない。
    PCollection<Weather> timestampedWeather = weather.apply(
        WithTimestamps.<Weather>of(w -> Instant.parse(w.timestamp))
            .withAllowedTimestampSkew(new Duration(Long.MAX_VALUE)));

    PCollection<Float> timestampedTemperature = timestampedWeather.apply(
        MapElements
            .into(TypeDescriptors.floats())
            .via(w -> w.temperatureC));

    PCollection<Float> windowedTemperature = timestampedTemperature.apply(
        Window.<Float>into(FixedWindows.of(Duration.standardDays(1))));

    // 日付ごとに平均気温を出すため、日付（ウィンドウの開始時刻）をキーにする
    PCollection<KV<Instant, Float>> keyedTemperature = windowedTemperature.apply(
        ParDo.of(new DoFn<Float, KV<Instant, Float>>() {
          @ProcessElement
          public void processElement(
              @Element Float temperature,
              IntervalWindow window,
              OutputReceiver<KV<Instant, Float>> out) {
            Instant keyDate = window.start();
            KV<Instant, Float> keyedTemperature = KV.of(keyDate, temperature);
            out.output(keyedTemperature);
          }
        }));

    PCollection<KV<Instant, Double>> meanTemperature = keyedTemperature.apply(
        Mean.<Instant, Float>perKey());

    // フォーマットして文字列化
    PCollection<String> meanTemperatureLine = meanTemperature.apply(
        MapElements
            .into(TypeDescriptors.strings())
            .via(kv -> kv.getKey().toString() + "\tmeanTemperature(daily):" + kv.getValue()));

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
