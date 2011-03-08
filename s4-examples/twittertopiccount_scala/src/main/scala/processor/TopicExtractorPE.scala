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

import io.s4.dispatcher.Dispatcher
import io.s4.processor.AbstractPE

import io.s4.example.twittertopiccount.event._

class TopicExtractorPE extends AbstractPE {
  @BeanProperty var id: String = _ 
  @BeanProperty var dispatcher: Dispatcher = _ 
  @BeanProperty var outputStreamName: String = _ 

  def processEvent(status: Status): Unit= {    
    var text = status.text
    if (text == null) return
    hashtags(text).foreach { x => 
      dispatcher.dispatchEvent(outputStreamName, new Topic(x, 1))
    }
  }

  def hashtags(in: String): Array[String]= in.split(" ").filter(_.startsWith("#")).map(_.replaceAll("[^a-zA-Z0-9]", ""))

  def output(): Unit= {
  }
}
