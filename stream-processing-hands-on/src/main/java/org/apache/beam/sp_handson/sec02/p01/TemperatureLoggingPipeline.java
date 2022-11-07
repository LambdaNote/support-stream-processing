package org.apache.beam.sp_handson.sec02.p01;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.mqtt.MqttIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class TemperatureLoggingPipeline {
  public static void main(String[] args) {
    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).withValidation().create());

    PCollection<byte[]> mqttInput = p.apply(MqttIO.read()
        .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
            "tcp://127.0.0.1:1883",
            "weather/kushiro")));

    mqttInput.apply(
        MqttIO.write()
            .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
                "tcp://127.0.0.1:1883",
                "beam/out")));

    p.run();
  }
}
