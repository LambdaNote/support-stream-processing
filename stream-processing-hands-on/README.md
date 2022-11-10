## 動作方法

### Kafka consumerで出力を待ち受ける

```console
% kafka-console-consumer --bootstrap-server localhost:9092 --topic beam-out
```

### パイプラインのプログラムを実行

`mainClass=`は適宜変更して実行。

```console
% mvn clean compile exec:java -Dexec.mainClass=org.apache.beam.sp_handson.sec02.p01.TemperaturePassthroughPipeline
```

### 気象データを入力

ファイル名、パスは適宜変更して実行。

- `--initial-timestamp`パラメータは、夏のファイル（`20220801-20220831-*.tsv`）を使う場合は`'2022-08-01T00:00:00.000+09:00'`にすると良い。
- `--speed`パラメータは`3600`（1時間分のログを1秒で処理）の他、`360000`（1時間分のログを0.01秒で処理）なども便利。
- `--dest-kafka-topic`パラメータは、どの地域のファイルを使うかにより使い分ける（`'weather-kushiro'`, `'weather-tokyo'`, `'weather-naha'`）。

```console
% replayman \
  --timed-by 'timestamp' \
  --initial-timestamp '2022-01-01T00:00:00.000+09:00' \
  --speed 3600 \
  --dest-kafka-bootstrap '127.0.0.1:9092' \
  --dest-kafka-topic 'weather-kushiro' \
  ../dataset-amedas/20220101-20220131-kushiro.tsv
```

以上で、`kafka-console-consumer`に何かしらの出力が出ることが期待される。
