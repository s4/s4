/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package io.s4.example.twittertopiccount.processor

import scala.math._
import scala.reflect.BeanProperty
import scala.collection.JavaConversions._

import java.util.concurrent.ConcurrentHashMap
import org.apache.log4j.Logger

import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._

import io.s4.persist.Persister
import io.s4.processor.AbstractPE

import io.s4.example.twittertopiccount.event._

class TopNTopicPE extends AbstractPE {
  @BeanProperty var id: String = _ 
  @BeanProperty var persistKey = "myapp:topNTopics"
  @BeanProperty var persister: Persister = _ 
  @BeanProperty var entryCount = 10
  @BeanProperty var persistTime = 0
  var topicMap = new ConcurrentHashMap[String, Int]

  def processEvent(topic: Topic): Unit= topicMap.put(topic.topic, topic.count)

  def output(): Unit= {

    // sort list of tuples by second value
    var sorted = topicMap.toList.sortBy(-_._2) 
    // limit to entryCount
    var tops = sorted.slice(0, min(entryCount, sorted.length))

    // use lift-json dsl to generate json
    val json = 
     ("topN" -> 
       tops.map { (x: (String, Int)) =>
	   (("topic" -> x._1) ~
	    ("count" -> x._2))
       }
     )

    try {
      persister.set(persistKey, pretty(render(json)), persistTime)
    } catch {
      case e: Exception => Logger.getLogger("s4").error(e)     
    }
  }

}



/*
    try {
      var jsonMessage = new JSONObject
      var jsonTopN = new JSONArray
      for (i <- 0 until(scala.math.min(entryCount, topN.length))) {
        var tne: TopNEntry = topN(i)
        var jsonEntry = new JSONObject()
        jsonEntry.put("topic", tne.topic)
        jsonEntry.put("count", tne.count)
        jsonTopN.put(jsonEntry)
      }
      jsonMessage.put("topN", jsonTopN)

  class TopNEntry(@BeanProperty var topic: String, @BeanProperty var count: Int) extends Ordered[TopNEntry] {
    def compare(that: TopNEntry)= this.count-that.count
    override def toString= "{topic:" + topic + ", count: " + count  + "}" 
  }
*/
