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

package org.apache.flink

import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer}
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.flink.streaming.api.windowing.time.Time



object StreamingJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60000) // checkpoint every minute
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // set the time characteristics for event time

    val properties = new Properties()
    // set kafka address and the consumer group id
    properties.setProperty("bootstrap.servers", "192.168.99.100:9092")

    properties.setProperty("group.id", "kafkaGroup")


    val stream= env
      .addSource(new FlinkKafkaConsumer[ObjectNode](List("clicks","displays").asJava, new JSONKeyValueDeserializationSchema(false), properties))
    val mapedStream= stream.map(node => node.findValue("value"))

      // we extract the event timestamp to get watermarks

    class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[JsonNode] {
      override def extractTimestamp(element: JsonNode, previousElementTimestamp: Long): Long = {
        element.get("timestamp").asLong()*1000
      }
      override def checkAndGetNextWatermark(lastElement: JsonNode, extractedTimestamp: Long): Watermark = {
        new Watermark(extractedTimestamp)
      }
    }

    // assignment of watermarks to the data stream
    val streamEventTime : DataStream[JsonNode] = mapedStream.assignTimestampsAndWatermarks(new PunctuatedAssigner)

    streamEventTime.print()

    // extract data with a tuple format ( eventType,uid,ip,timestamp,impressionId)
    def find_Elements (jsonMap: JsonNode) : (String,String, String, Long,String) = {
      val eventType = jsonMap.findValue("eventType").toString
      val uid = jsonMap.findValue("uid").toString
      val ip = jsonMap.findValue("ip").toString
      val timestamp = jsonMap.findValue("timestamp").asLong()
      val impressionId = jsonMap.findValue("impressionId").toString
      return ( eventType,uid,ip,timestamp,impressionId)
    }

    // Pattern 1 : how many Uid per a unique IP adress ?

        // first we key the stream by ip , after we key by uid => each row corresponds (ip, uid, count of ip_uid)
    val count_ip_uid = streamEventTime
      .map(x => (find_Elements(x), 1.0))
      .keyBy { elem => elem match { case ((_,_, ip, _,_), _) => ip }}
      .keyBy{ elem => elem match { case ((_,uid, _, _,_), _) => uid }}
      .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(30)))
      .allowedLateness(Time.seconds(5))
      .reduce{ (v1, v2) => (v1._1,v1._2+v2._2) }

        // now we map each row to 1 and calculate the number of UID per a unique IP
        // at the end we filter on suspicious events , which UID count is > 2
        // the result format is (ip, uid_count>2 )

    val count_uid_By_IP = count_ip_uid.map(x => (x._1._3 ,x._1._2, 1.0))
      .map(x => (x._1,x._3))
      .keyBy {0}
      //the sliding window is useful when there are event spikes, for example a huge number of clicks within the time window.
      // It allows a distribution in the events processing
      .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(30)))
      .allowedLateness(Time.seconds(5)) // allow a events lateness of 5 seconds
      .reduce{ (v1, v2) => (v1._1,v1._2+v2._2) }
      .filter( x => x._2 > 2.0)

    //count_uid_By_IP.print()

    // Pattern 2 : how many Ip addresses per a unique userID ?

        // first we key the stream by uid , after we key by ip => each row corresponds (uid, ip, count of uid_ip)
    val count_uid_ip = streamEventTime
      .map(x => (find_Elements(x), 1.0))
      .keyBy{ elem => elem match { case ((_,uid, _, _,_), _) => uid }}
      .keyBy { elem => elem match { case ((_,_, ip, _,_), _) => ip }}
      .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(30)))
      .allowedLateness(Time.seconds(5))
      .reduce{ (v1, v2) => (v1._1,v1._2+v2._2) }


    // now we map each row to 1 , we take only (ip, 1) and we calculate the number of IPs per a unique UID
    // at the end we filter on suspicious events , which Ip count is > 1
    // the result format is (uid, ip_count>1 )

    val count_Ip_By_uid = count_uid_ip.map(x => (x._1._2 ,x._1._3, 1.0))
      .map(x => (x._1,x._3))
      .keyBy {0}
      .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(30)))
      .allowedLateness(Time.seconds(5))
      .reduce{ (v1, v2) => (v1._1,v1._2+v2._2) }
      .filter( x => x._2 > 1.0)

    //count_Ip_By_uid.print()

    //Pattern 3 : we calculate CTR per UID
        // first we need to split streamEventTime by clicks and displays
        // we count clicks and displays per uid

    val click_stream= streamEventTime.filter(node => node.get("eventType").asText.equals("click") ).map(node => (node.get("eventType").asText(),node.get("uid").asText() , 1.0)).keyBy(1).window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .reduce{ (v1, v2) => (v1._1,v1._2,v1._3+v2._3) }
    val display_stream = streamEventTime.filter(node => node.get("eventType").asText.equals("display") ).map(node => (node.get("eventType").asText(),node.get("uid").asText() , 1.0)).keyBy(1).window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .reduce{ (v1, v2) => (v1._1,v1._2,v1._3+v2._3) }

        // now we join on uid and calculate the CTR per uid
        // the result is filtered on suspicious events ( CTR> 0.5 and clicks_count>1 and displays_count >1)
        // each result row corresponds to (uid,eventType_clicks, clicks_count,eventType_displays,displays_count, CTR)

    val CTR_pattern= click_stream.join(display_stream).where(_._2).equalTo(_._2)
      .window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(30)))
      .allowedLateness(Time.seconds(5))
      .apply { (e1, e2) => ( e1._2,e1._1,e1._3, e2._1, e2._3,e1._3/e2._3) }.filter(x =>  x._6 > 0.5 & x._3 != 1.0 & x._5 != 1.0)


    // we output the patterns

    count_uid_By_IP.writeAsText("C:/Program Files/Docker Toolbox/projet/paris-dauphine-master/docker/kafka-zk/Pattern_1.txt",org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)
    count_Ip_By_uid.writeAsText("C:/Program Files/Docker Toolbox/projet/paris-dauphine-master/docker/kafka-zk/Pattern_2.txt",org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)
    CTR_pattern.writeAsText("C:/Program Files/Docker Toolbox/projet/paris-dauphine-master/docker/kafka-zk/Pattern_3.txt",org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)
    streamEventTime.map(x => find_Elements(x)).writeAsText("C:/Program Files/Docker Toolbox/projet/paris-dauphine-master/docker/kafka-zk/all_events.txt",org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)


    env.execute("Flink Streaming for fraud detection")
  }
}
