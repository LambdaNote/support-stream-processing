package org.apache.beam.sp_handson.sec02.p02;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

// アメダス気象情報
@DefaultSchema(JavaFieldSchema.class)
public class Weather {
    // 観測日時
    public final String timestamp;
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
        this.timestamp = timestamp;
        this.temperatureC = temperatureC;
        this.rainfallMm = rainfallMm;
    }

    public String toLine() {
        return this.timestamp + "\ttemperature:" + this.temperatureC + "\trainfall:" + this.rainfallMm;
    }
}
