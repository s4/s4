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
package io.s4.example.twittertopiccount;

import io.s4.dispatcher.Dispatcher;
import io.s4.dispatcher.EventDispatcher;
import io.s4.processor.AbstractPE;

public class TopicExtractorPE extends AbstractPE {
    private String id;
    private EventDispatcher dispatcher;
    private String outputStreamName;

    public EventDispatcher getDispatcher() {
        return dispatcher;
    }

    public void setDispatcher(EventDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public String getOutputStreamName() {
        return outputStreamName;
    }

    public void setOutputStreamName(String outputStreamName) {
        this.outputStreamName = outputStreamName;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void processEvent(Status status) {
        String text = status.getText();
        if (text == null) {
            return;
        }

        int textLength = text.length();
        int index = 0;
        int prevIndex = 0;
        while ((index = text.indexOf('#', prevIndex)) != -1) {
            prevIndex = index + 1;
            if (prevIndex == textLength) { // if hash is the last character
                break; // get out
            }
            StringBuffer sb = new StringBuffer();
            for (int i = index + 1; i < textLength; i++) {
                char ch = text.charAt(i);
                if (!Character.isLetterOrDigit(ch)) {
                    break;
                }
                sb.append(ch);
            }

            if (sb.length() == 0) {
                continue;
            }

            TopicSeen topicSeen = new TopicSeen(sb.toString().toLowerCase(), 1);
            dispatcher.dispatchEvent(outputStreamName, topicSeen);
        }
    }

    @Override
    public void output() {
        // TODO Auto-generated method stub

    }

    @Override
    public String getId() {
        return this.id;
    }

    static class DummyDispatcher extends Dispatcher {
        public void dispatchEvent(String streamName, Object event) {
            System.out.println(event);
        }
    }

    public static void main(String args[]) {
        TopicExtractorPE te = new TopicExtractorPE();
        te.setDispatcher(new DummyDispatcher());
        te.setOutputStreamName("test");

        Status status = new Status();
        status.setText("Hey this is a test");
        te.processEvent(status);

        status.setText("This is an edge test #");
        te.processEvent(status);

        status.setText("#GLOB this is a test");
        te.processEvent(status);

        status.setText("Hey there #FLOB, this is a test #GLOB");
        te.processEvent(status);
    }
}
