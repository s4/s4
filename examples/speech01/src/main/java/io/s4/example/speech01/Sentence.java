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
package io.s4.example.speech01;

public class Sentence {
    private long id;
    private long speechId;
    private String text;
    private long time;
    private String location;
    
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getSpeechId() {
        return speechId;
    }

    public void setSpeechId(long speechId) {
        this.speechId = speechId;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("{id:")
          .append(id)
          .append(",speechId:")
          .append(speechId)         
          .append(",text:")
          .append(text)       
          .append(",time:")
          .append(time) 
          .append(",location:")
          .append(location)               
          .append("}");

        return sb.toString();
    }
}
