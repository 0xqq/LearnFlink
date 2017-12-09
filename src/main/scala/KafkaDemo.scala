import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.hadoop.io.{IntWritable, Text}

object KafkaDemo {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.disableSysoutLogging

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")

    val consumer = new FlinkKafkaConsumer010[String](
      "test",
      new SimpleStringSchema,
      properties)

//    consumer.setStartFromGroupOffsets()
//    consumer.setStartFromLatest()

    val message: DataStream[String] = env
      .addSource(consumer)

    /*
     window(TumblingEventTimeWindows.of(Time.seconds(10)))
     Caused by: java.lang.RuntimeException: Record has Long.MIN_VALUE timestamp (= no timestamp marker). Is the time characteristic set to 'ProcessingTime', or did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'?
     */


    val ret = message
      .map((_,1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .sum(1)
      .map(x => s"${x._1}: ${x._2}")

     // sink to kafka
    val sink = new FlinkKafkaProducer010("sink-test", new SimpleStringSchema(), properties)

    ret.addSink(sink)

    env.execute()

  }

}



/*
本地
安装环境
Brew info apache-flink

启动repl
Cd /usr/local/Cellar/apache-flink/1.3.2/libexec
/bin/start-local.sh

bin/start-scala-shell.sh local

添加jar
--addclasspath xxx.jar

添加多个jar
--addclasspath xx1.jar:xx2.jar

kafka
0.10也需要提交0.9的jar!!!
/usr/local/Cellar/apache-flink/1.3.2/libexec/bin/start-scala-shell.sh local --addclasspath /Users/zzz24512653/jars/flink-connector-kafka-0.10_2.11-1.3.2.jar:/Users/zzz24512653/jars/flink-connector-kafka-base_2.11-1.3.2.jar:/Users/zzz24512653/jars/kafka-clients-0.10.2.1.jar:/Users/zzz24512653/jars/flink-connector-kafka-0.9_2.10-1.3.2.jar

/usr/local/Cellar/apache-flink/1.3.2/libexec/bin/start-scala-shell.sh local --addclasspath /Users/zzz24512653/jars/flink-connector-kafka-0.10_2.11-1.3.2.jar:/Users/zzz24512653/jars/flink-connector-kafka-base_2.11-1.3.2.jar:/Users/zzz24512653/jars/kafka-clients-0.10.2.1.jar:/Users/zzz24512653/jars/flink-connector-kafka-0.9_2.10-1.3.2.jar


提交Jar
flink run -c xxx examples/streaming/SocketWindowWordCount.jar --port 9000


bin/flink run -c xxx xxx.jar



/opt/tiger/flink_deploy/deploy/flink-dev/bin/flink run -m yarn-cluster -c RemoteWordCount target/learn-flink-0.0.1-SNAPSHOT-jar-with-dependencies.jar —hostname 10.3.23.41 —port 11006


/opt/tiger/flink_deploy/deploy/flink-dev/bin/flink run -m  yarn-client -yn 2 -yjm 1024 -ytm 1024 /opt/tiger/flink_deploy/deploy/flink-dev/examples/batch/WordCount.jar



Local
  ./bin/flink run -m yarn-cluster -yn 4 -yjm 1024 -ytm 4096 ./examples/batch/WordCount.jar

yarn-client

Flink session
./bin/yarn-session.sh -n 4 -jm 1024 -tm 4096
*/