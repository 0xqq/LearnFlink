/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * Implements a streaming windowed version of the "WordCount" program.
  *
  * This program connects to a server socket and reads strings from the socket.
  * The easiest way to try this out is to open a text sever (at port 12345)
  * using the ''netcat'' tool via
  * {{{
  * nc -l 12345
  * }}}
  * and run this example with the hostname and the port as arguments..
  */
object RestartDemo {

  /** Main program method */
  def main(args: Array[String]) : Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    env.getConfig.disableSysoutLogging()

    env.setRestartStrategy(RestartStrategies.failureRateRestart(
      1, // max failures per unit
      Time.of(5, TimeUnit.MINUTES), //time interval for measuring failure rate
      Time.of(10, TimeUnit.SECONDS) // delay
    ))

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")

    val consumer = new FlinkKafkaConsumer010[String](
      "test",
      new SimpleStringSchema,
      properties)

    val message: DataStream[String] = env
      .addSource(consumer)

    message.map { w =>
      s"word: ${w.toDouble}"
    }.print()

    env.execute("Socket Window WordCount")
  }

}