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

package io.s4.example.twittertopiccount.util

import java.io.InputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import java.io.IOException
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.HttpException
import org.apache.commons.httpclient.HttpStatus
import org.apache.commons.httpclient.HttpURL
import org.apache.commons.httpclient.UsernamePasswordCredentials
import org.apache.commons.httpclient.auth.AuthScope
import org.apache.commons.httpclient.HttpMethod
import org.apache.commons.httpclient.methods.GetMethod

import java.util.concurrent.LinkedBlockingQueue

import scala.reflect.BeanProperty
import scala.concurrent.ops._

class TwitterStreamClient {

  @BeanProperty var user: String = _   
  @BeanProperty var pass: String = _   
  @BeanProperty var url: String = _   
  @BeanProperty var backoffTime = 1000
  @BeanProperty var maxBackoffTime = 30 * 1000
  @BeanProperty var queue: LinkedBlockingQueue[String] = _ 

  def init() = {
    spawn {
      val http = new HttpClient
      val getter = new GetMethod(url)
      http.getState.setCredentials(this.getAuthScope(getter), new UsernamePasswordCredentials(user + ":" + pass))
      while(!Thread.interrupted()) {
	try {
	  http.executeMethod(getter)
	  if (getter.getStatusCode() != HttpStatus.SC_OK) {
            throw new HttpException("HTTP status: " + getter.getStatusCode() + " " + getter.getStatusLine())
	  }
	  this.process(getter.getResponseBodyAsStream())	
	} catch {
	  case _ => {
	    try {
	      Thread.sleep(backoffTime)
	    } catch {
	      case e: InterruptedException =>  Thread.currentThread().interrupt()
	    }
            backoffTime = backoffTime * 2;
            if (backoffTime > maxBackoffTime) {
              backoffTime = maxBackoffTime;
            }	
	  }
	} finally {
	  getter.releaseConnection
	}
      }
    }
  }

  def getAuthScope(method: HttpMethod) = {
    val url = new HttpURL(method.getURI.toString)
    new AuthScope(url.getHost, url.getPort)
  }

  def process(is: InputStream) = {
    val reader: BufferedReader = new BufferedReader(new InputStreamReader(is, "UTF-8"))
    var line = reader.readLine()
    while (line != null) {
      queue.add(line)
      line = reader.readLine()
    }
    is.close
  }

}
