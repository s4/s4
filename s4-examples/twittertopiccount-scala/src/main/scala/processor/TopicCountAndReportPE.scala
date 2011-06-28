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

import org.apache.log4j.Logger
import scala.reflect.BeanProperty

import io.s4.dispatcher.EventDispatcher
import io.s4.processor.AbstractPE

import io.s4.example.twittertopiccount.event._

class TopicCountAndReportPE extends AbstractPE {
  @BeanProperty var dispatcher: EventDispatcher = _ 
  @BeanProperty var outputStreamName: String = _ 
  @BeanProperty var threshold: Int = _
  @BeanProperty var count: Int = _

  def processEvent(topic: Topic): Unit= {
    count += topic.count;
  }

  def output(): Unit= {
    if (count < threshold) return  
    var topic: Topic = new Topic(this.getKeyValue.get(0).toString, count)
    topic.reportKey = "1"
    dispatcher.dispatchEvent(outputStreamName, topic)
  }
}
