import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.hadoop.io.{IntWritable, Text}


class RedisExampleMapper extends RedisMapper[(String, String)]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET)
  }

  override def getKeyFromData(data: (String, String)): String = data._1

  override def getValueFromData(data: (String, String)): String = data._2
}

object RedisDemo {

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

    val ret = message
      .map((_,1))
      .keyBy(0)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .sum(1)
      .map(x => (x._1, x._2.toString))

    import redis.clients.jedis.JedisPool
    import redis.clients.jedis.JedisPoolConfig

    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("127.0.0.1")
      .setPort(1)
      .build()

    ret.addSink(new RedisSink[(String, String)](conf, new RedisExampleMapper))

    env.execute()

  }

}
