package org.apache.beam.sp_handson.sec03.p01;

import java.util.Optional;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sp_handson.sec02.p02.Weather;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Instant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

// KafkaIO.Read で使う、Event timeの割当ポリシー（のファクトリ）。
// 値がJSON文字列であること、Weatherクラスにマッピングできることを前提とし動作。
//
// KafkaレコードをいちいちJSONパースするのでパフォーマンス状は良い実装とは言えない。
public class WeatherTimestampPolicyFactory<K> implements TimestampPolicyFactory<K, String> {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public TimestampPolicy<K, String> createTimestampPolicy(
      TopicPartition tp, Optional<Instant> previousWatermark) {
    return new TimestampPolicy<K, String>() {
      Instant lastTimestamp = Instant.EPOCH;

      @Override
      public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord<K, String> rec) {
        String jsonWeather = rec.getKV().getValue();
        Weather weather;
        try {
          weather = objectMapper.readValue(jsonWeather, Weather.class);
        } catch (JsonProcessingException e) {
          e.printStackTrace();
          return Instant.EPOCH;
        }
        this.lastTimestamp = Instant.parse(weather.timestamp);
        return this.lastTimestamp;
      }

      @Override
      public Instant getWatermark(PartitionContext ctx) {
        Instant prevWatermark = previousWatermark.orElse(Instant.EPOCH);
        // ウォーターマークは単調増加になるようにする
        if (this.lastTimestamp.getMillis() > prevWatermark.getMillis()) {
          return this.lastTimestamp;
        } else {
          return prevWatermark;
        }
      }
    };
  }
}
