package org.apache.beam.sp_handson.sec03.p03;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
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

// 30℃以上の気温が継続した日数をカウントするパイプライン
public class HeatWavePipeline {

  // アメダス観測のJSON文字列をWeatherクラスにマッピングするため
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static void main(String[] args) {
    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).withValidation().create());

    // Kafkaソース入力
    PCollection<KV<Long, String>> kafkaInput = p.apply(
        KafkaIO.<Long, String>read()
            .withBootstrapServers("localhost:9092")
            // 東京用のトピックを使用
            .withTopic("weather-tokyo")
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
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

    // 30℃以上の気温だけをフィルタリング
    PCollection<Float> temperatureOver30 = temperature.apply(
        ParDo.of(new DoFn<Float, Float>() {
          @ProcessElement
          public void processElement(
              @Element Float t,
              OutputReceiver<Float> out) {
            if (t >= 30.0) {
              out.output(t);
            }
          }
        }));

    // Event time-basedなセッションウィンドウ（セッション持続時間 = 1日）を構築
    PCollection<Float> windowedTemperature = temperatureOver30.apply(
        Window.<Float>into(
            Sessions.withGapDuration(Duration.standardDays(1))));

    // // ウィンドウ情報からウィンドウ開始点を取得
    // PCollection<KV<Instant, Float>> windowedTemperatureWithDate =
    // windowedTemperature.apply(
    // ParDo.of(new DoFn<Float, KV<Instant, Float>>() {
    // @ProcessElement
    // public void processElement(
    // @Element Float temperature,
    // IntervalWindow window,
    // OutputReceiver<KV<Instant, Float>> out) {
    // Instant winEnd = window.maxTimestamp();

    // System.out.println(winEnd + "\t" + temperature);

    // out.output(KV.of(winEnd, temperature));
    // }
    // }));

    // ウィンドウ開始点毎（セッションウィンドウ毎）に、
    // 各ウィンドウでのイベントの個数（30℃以上が継続した日数）をカウント
    PCollection<Long> eventCounts = windowedTemperature.apply(
        Combine.globally(Count.<Float>combineFn()).withoutDefaults());

    // フォーマットして文字列化
    PCollection<String> meanTemperatureLine = eventCounts.apply(
        MapElements
            .into(TypeDescriptors.strings())
            .via(cnt -> "leftmost datetime:"
                // 日本時間での日時
                // + cnt.getKey().toDateTime(DateTimeZone.forID("+09:00")).toString()
                + "\tcount:"
                + cnt));

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
