package org.apache.beam.sp_handson.sec02.p01;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.mqtt.MqttIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TemperatureLoggingPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(TemperatureLoggingPipeline.class);

  public static void main(String[] args) {
    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).withValidation().create());

    PCollection<byte[]> mqttInput = p.apply(MqttIO.read()
        .withConnectionConfiguration(MqttIO.ConnectionConfiguration.create(
            "tcp://127.0.0.1:1883",
            "weather/kushiro")));

    mqttInput.apply(ParDo.of(new DoFn<byte[], Void>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        String input = new String(c.element());
        LOG.info(input);
      }
    }));

    p.run();
  }
}
