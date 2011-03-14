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

package io.s4.example.twittertopiccount.listener

import scala.reflect.BeanProperty
import scala.concurrent.ops._
import scala.collection.JavaConversions._
import org.apache.log4j.Logger

import java.util.concurrent._
import java.util.HashSet
import java.util.concurrent.LinkedBlockingQueue

import io.s4.collector.EventWrapper
import io.s4.listener.EventHandler
import io.s4.listener.EventProducer

import net.liftweb.json._
import net.liftweb.json.Extraction._

import io.s4.example.twittertopiccount.event._

class TwitterStreamListener extends EventProducer {

  @BeanProperty var queue: LinkedBlockingQueue[String] = _ 
  @BeanProperty var threads = 10
  @BeanProperty var streamName: String = _

  var handlers = new HashSet[EventHandler]
  override def addHandler(h: EventHandler)= handlers.add(h)
  override def removeHandler(h: EventHandler)= handlers.remove(h)
  
  def init()= {
    spawn {
      var pool = Executors.newFixedThreadPool(10)
      for (i <- 1 to threads) {
	pool.execute(new Dequeuer)
      }
    }
  }

  class Dequeuer extends Runnable {

    implicit val formats = DefaultFormats

    def run()= {
      while(!Thread.interrupted()) {
	try {
	  var message = queue.take()
	  var json = JsonParser.parse(message)
	  //println(Printer.pretty(JsonAST.render(json)))
	  var status = json.extract[Status]
	  // println(status)
	  var ew = new EventWrapper(streamName, status, null)
	  try {
	    handlers.foreach(_.processEvent(ew)) 
	  } catch {
	    case e: Exception => Logger.getLogger("s4").error("Exception in raw event handler", e) 
	  }
	} catch {
	  case e: InterruptedException =>  Thread.currentThread().interrupt()
	  case _ => 
	}
      }
    }

  }

}
