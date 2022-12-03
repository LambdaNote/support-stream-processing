package org.apache.beam.sp_handson.sec03.p04;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sp_handson.sec02.p02.Weather;
import org.apache.beam.sp_handson.sec03.p01.WeatherTimestampPolicyFactory;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

// 全データ通した平均気温を処理時間軸で1秒ごとに更新し出力するパイプライン
public class AvgTemperatureUpdatedPer1Hour {

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

    // グローバルウィンドウに対し、処理時間軸で1秒ごとのトリガーを設定
    PCollection<Float> windowedTemperature = temperature.apply(
        Window.<Float>into(new GlobalWindows())
            .triggering(
                Repeatedly.forever(
                    AfterProcessingTime.pastFirstElementInPane()
                        .plusDelayOf(Duration.standardSeconds(1))))
            // 今までのトリガーされた値と無関係に最新値を出力するモード
            .discardingFiredPanes());

    // トリガーが発動された際に実行される集約演算（平均）
    PCollection<Double> meanTemperature = windowedTemperature.apply(
        Mean.<Float>globally().withoutDefaults());

    // フォーマットして文字列化
    PCollection<String> rainfallCountLine = meanTemperature.apply(
        ParDo.of(new DoFn<Double, String>() {
          @ProcessElement
          public void processElement(
              @Element Double meanT,
              OutputReceiver<String> out) {
            String s = "currentMeanTemperature:" + meanT;
            out.output(s);
          }
        }));

    // Kafkaシンク出力
    rainfallCountLine.apply(KafkaIO.<Void, String>write().withBootstrapServers("localhost:9092").withTopic("beam-out")
        .withValueSerializer(StringSerializer.class).values());

    p.run();
  }
}
