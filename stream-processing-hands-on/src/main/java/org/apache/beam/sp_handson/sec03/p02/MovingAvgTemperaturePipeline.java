package org.apache.beam.sp_handson.sec03.p02;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sp_handson.sec02.p02.Weather;
import org.apache.beam.sp_handson.sec03.p01.WeatherTimestampPolicyFactory;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MovingAvgTemperaturePipeline {

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
            // Event time割当を行う
            .withTimestampPolicyFactory(new WeatherTimestampPolicyFactory<>())
            .withReadCommitted()
            .withoutMetadata());

    // KV<Long, String> のバリュー部分をJSONとしてパースし、Weatherクラスにマッピング
    PCollection<Weather> weather = kafkaInput.apply(
        ParDo.of(new DoFn<KV<Long, String>, Weather>() {
          @ProcessElement
          public void processElement(@Element KV<Long, String> rawWeather,
              OutputReceiver<Weather> out)
              throws JsonProcessingException {
            String jsonWeather = rawWeather.getValue();
            Weather weather = objectMapper.readValue(jsonWeather, Weather.class);
            out.output(weather);
          }
        }));

    // 気象情報から気温だけを抽出
    PCollection<Float> temperature = weather.apply(
        MapElements.into(TypeDescriptors.floats())
            .via(w -> w.temperatureC));

    // Event time-basedなスライディングウィンドウを構築
    PCollection<Float> windowedTemperature = temperature.apply(
        Window.<Float>into(
            // ウィンドウ幅: 3時間
            SlidingWindows.of(Duration.standardHours(3))
                // オフセット: 1時間
                .every(Duration.standardHours(1))));

    // ウィンドウ情報からウィンドウ開始点を取得
    PCollection<KV<Instant, Float>> windowedTemperatureWithDate = windowedTemperature.apply(
        ParDo.of(new DoFn<Float, KV<Instant, Float>>() {
          @ProcessElement
          public void processElement(
              @Element Float temperature,
              IntervalWindow window,
              OutputReceiver<KV<Instant, Float>> out) {
            Instant keyDate = window.start();
            out.output(KV.of(keyDate, temperature));
          }
        }));

    // ウィンドウ開始点毎に、各ウィンドウで気温の平均値を計算
    PCollection<KV<Instant, Double>> meanTemperature = windowedTemperatureWithDate.apply(
        Mean.<Instant, Float>perKey());

    // フォーマットして文字列化
    PCollection<String> meanTemperatureLine = meanTemperature.apply(
        MapElements
            .into(TypeDescriptors.strings())
            .via(mean -> "leftmost datetime:"
                // 日本時間での日時
                + mean.getKey().toDateTime(DateTimeZone.forID("+09:00")).toString()
                + "\tmeanTemperature(per 3-hour):"
                + mean.getValue()));

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
