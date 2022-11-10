package org.apache.beam.sp_handson.sec02.p02;

import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.joda.time.Instant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

// アメダス気象情報
@DefaultSchema(JavaBeanSchema.class)
public class Weather {
    // 観測日時 (event timeとして利用)
    public final Instant timestamp;
    // 気温 [℃]
    public final float temperatureC;
    // 降水量 [mm]
    public final float rainfallMm;

    @JsonCreator
    @SchemaCreate
    public Weather(
            @JsonProperty("timestamp") String timestamp,
            @JsonProperty("temperature [°C]") float temperatureC,
            @JsonProperty("rainfall [mm]") float rainfallMm) {
        this.timestamp = Instant.parse(timestamp);
        this.temperatureC = temperatureC;
        this.rainfallMm = rainfallMm;
    }

    public String toLine() {
        return this.timestamp.toString() + "\ttemperature:" + this.temperatureC + "\trainfall:" + this.rainfallMm;
    }
}
