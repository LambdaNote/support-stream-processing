package org.apache.beam.sp_handson.sec03.p03;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
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

// 降水が持続した時間をカウントするパイプライン
public class ContinuousRainPipeline {

  // アメダス観測のJSON文字列をWeatherクラスにマッピングするため
  private static final ObjectMapper objectMapper = new ObjectMapper();

  // セッション持続時間（セッションウィンドウは開区間なので3600秒だと隣接データに届かない）
  private static final Duration sessionDuration = Duration.millis(60 * 60 * 1000 + (long) 1);

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

    // 気象情報から降水量だけを抽出
    PCollection<Float> rainfall = weather.apply(
        MapElements.into(TypeDescriptors.floats())
            .via(w -> w.rainfallMm));

    // 降水時だけをフィルタリング
    PCollection<Float> rainfallOver0 = rainfall.apply(
        ParDo.of(new DoFn<Float, Float>() {
          @ProcessElement
          public void processElement(
              @Element Float r,
              OutputReceiver<Float> out) {
            if (r > 0.0) {
              out.output(r);
            }
          }
        }));

    // Event time-basedなセッションウィンドウ（セッション持続時間 = 1時間）を構築
    PCollection<Float> windowedRainfall = rainfallOver0.apply(
        Window.<Float>into(
            Sessions.withGapDuration(sessionDuration)));

    // ウィンドウ開始点毎（セッションウィンドウ毎）に、
    // 各ウィンドウでのイベントの個数（降水がある時間数）をカウント
    PCollection<Long> eventCounts = windowedRainfall.apply(
        Combine.globally(Count.<Float>combineFn()).withoutDefaults());

    // フォーマットして文字列化
    PCollection<String> rainfallCountLine = eventCounts.apply(
        ParDo.of(new DoFn<Long, String>() {
          @ProcessElement
          public void processElement(
              @Element Long cnt,
              @Timestamp Instant ts,
              OutputReceiver<String> out) {
            String s = "rainStoppedAt:"
                // 日本時間での日時
                + ts.toDateTime(DateTimeZone.forID("+09:00")).toString()
                + "\trainContinued[h]:"
                + cnt;
            out.output(s);
          }
        }));

    // Kafkaシンク出力
    rainfallCountLine.apply(KafkaIO.<Void, String>write().withBootstrapServers("localhost:9092").withTopic("beam-out")
        .withValueSerializer(StringSerializer.class).values());

    p.run();
  }
}
