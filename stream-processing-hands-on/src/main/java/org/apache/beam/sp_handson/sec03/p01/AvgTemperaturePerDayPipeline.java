package org.apache.beam.sp_handson.sec03.p01;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sp_handson.sec02.p02.Weather;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTimeZone;
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

        // 気象情報から日付と気温だけを抽出
        PCollection<KV<String, Float>> temperatureWithDate = weather.apply(
                ParDo.of(new DoFn<Weather, KV<String, Float>>() {
                    @ProcessElement
                    public void processElement(
                            @Element Weather w,
                            OutputReceiver<KV<String, Float>> out) {
                        // 日本時間での日付
                        String date = Instant.parse(w.timestamp)
                                .toDateTime(DateTimeZone.forID("+09:00"))
                                .toLocalDate().toString();
                        out.output(KV.of(date, w.temperatureC));
                    }
                }));

        // Event time軸で1日毎の固定幅ウィンドウを構築
        PCollection<KV<String, Float>> windowedTemperatureWithDate = temperatureWithDate.apply(
                Window.<KV<String, Float>>into(
                        FixedWindows.of(Duration.standardDays(1))
                                // ウィンドウの開始日時はUTC原点の0時になるので日本時間の0時にずらす。
                                // -9時間ずらしたいが、負数指定ができないので 24 - 9 = 15 時間ずらす。
                                .withOffset(Duration.standardHours(15))));

        // 日付毎に、各ウィンドウで気温の平均値を計算
        PCollection<KV<String, Double>> meanTemperature = windowedTemperatureWithDate.apply(
                Mean.<String, Float>perKey());

        // フォーマットして文字列化
        PCollection<String> meanTemperatureLine = meanTemperature.apply(
                MapElements
                        .into(TypeDescriptors.strings())
                        .via(meanByDate -> "date:" + meanByDate.getKey()
                                + "\tmeanTemperature(daily):"
                                + meanByDate.getValue()));

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
