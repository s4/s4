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

import io.s4.persist.Persister;
import io.s4.processor.AbstractPE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;

public class TopNTopicPE extends AbstractPE {
    private String id;
    private Persister persister;
    private int entryCount = 10;
    private Map<String, Integer> topicMap = new ConcurrentHashMap<String, Integer>();
    private int persistTime;
    private String persistKey = "myapp:topNTopics";

    public void setId(String id) {
        this.id = id;
    }

    public Persister getPersister() {
        return persister;
    }

    public void setPersister(Persister persister) {
        this.persister = persister;
    }

    public int getEntryCount() {
        return entryCount;
    }

    public void setEntryCount(int entryCount) {
        this.entryCount = entryCount;
    }

    public int getPersistTime() {
        return persistTime;
    }

    public void setPersistTime(int persistTime) {
        this.persistTime = persistTime;
    }

    public String getPersistKey() {
        return persistKey;
    }

    public void setPersistKey(String persistKey) {
        this.persistKey = persistKey;
    }

    public void processEvent(TopicSeen topicSeen) {
        topicMap.put(topicSeen.getTopic(), topicSeen.getCount());
    }

    public ArrayList<TopNEntry> getTopTopics() {
        if (entryCount < 1) 
            return null;

        ArrayList<TopNEntry> sortedList = new ArrayList<TopNEntry>();

        for (String key : topicMap.keySet()) {
            sortedList.add(new TopNEntry(key, topicMap.get(key)));
        }

        Collections.sort(sortedList);

        // truncate: Yuck!!
        // unfortunately, Kryo cannot deserialize RandomAccessSubList
        // if we use ArrayList.subList(...)
        while (sortedList.size() > entryCount)
            sortedList.remove(sortedList.size() - 1);

        return sortedList;
    }

    @Override
    public void output() {
        List<TopNEntry> sortedList = new ArrayList<TopNEntry>();

        for (String key : topicMap.keySet()) {
            sortedList.add(new TopNEntry(key, topicMap.get(key)));
        }

        Collections.sort(sortedList);

        try {
            JSONObject message = new JSONObject();
            JSONArray jsonTopN = new JSONArray();

            for (int i = 0; i < entryCount; i++) {
                if (i == sortedList.size()) {
                    break;
                }
                TopNEntry tne = sortedList.get(i);
                JSONObject jsonEntry = new JSONObject();
                jsonEntry.put("topic", tne.getTopic());
                jsonEntry.put("count", tne.getCount());
                jsonTopN.put(jsonEntry);
            }
            message.put("topN", jsonTopN);
            persister.set(persistKey, message.toString()+"\n", persistTime);
        } catch (Exception e) {
            Logger.getLogger("s4").error(e);
        }
    }

    @Override
    public String getId() {
        return this.id;
    }

    public static class TopNEntry implements Comparable<TopNEntry> {
        public TopNEntry(String topic, int count) {
            this.topic = topic;
            this.count = count;
        }

        public TopNEntry() {}

        String topic = null;
        int count = 0;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public int compareTo(TopNEntry topNEntry) {
            if (topNEntry.getCount() < this.count) {
                return -1;
            } else if (topNEntry.getCount() > this.count) {
                return 1;
            }
            return 0;
        }

        public String toString() {
            return "topic:" + topic + " count:" + count;
        }
    }
}
