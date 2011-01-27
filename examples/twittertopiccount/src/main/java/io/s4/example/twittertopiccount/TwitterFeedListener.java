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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.util.EncodingUtil;

import io.s4.collector.EventWrapper;
import io.s4.listener.EventHandler;
import io.s4.listener.EventProducer;

public class TwitterFeedListener implements EventProducer, Runnable {
    private String userid;
    private String password;
    private String urlString;
    private long maxBackoffTime = 30 * 1000; // 5 seconds
    private long messageCount = 0;
    private long blankCount = 0;
    private String streamName;

    private LinkedBlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>();
    private Set<io.s4.listener.EventHandler> handlers = new HashSet<io.s4.listener.EventHandler>();

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setUrlString(String urlString) {
        this.urlString = urlString;
    }

    public void setMaxBackoffTime(long maxBackoffTime) {
        this.maxBackoffTime = maxBackoffTime;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public void init() {
        for (int i = 0; i < 12; i++) {
            Dequeuer dequeuer = new Dequeuer(i);
            Thread t = new Thread(dequeuer);
            t.start();
        }
        (new Thread(this)).start();
    }

    public void run() {
        long backoffTime = 1000;
        while (!Thread.interrupted()) {
            try {
                connectAndRead();
            } catch (Exception e) {
                Logger.getLogger("s4").error("Exception reading feed", e);
                try {
                    Thread.sleep(backoffTime);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                backoffTime = backoffTime * 2;
                if (backoffTime > maxBackoffTime) {
                    backoffTime = maxBackoffTime;
                }
            }
        }
    }

    public void connectAndRead() throws Exception {
        URL url = new URL(urlString);

        URLConnection connection = url.openConnection();
        String userPassword = userid + ":" + password;
        String encoded = EncodingUtil.getAsciiString(Base64.encodeBase64(EncodingUtil.getAsciiBytes(userPassword)));
        connection.setRequestProperty("Authorization", "Basic " + encoded);
        connection.connect();

        InputStream is = connection.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);

        String inputLine = null;
        while ((inputLine = br.readLine()) != null) {
            if (inputLine.trim().length() == 0) {
                blankCount++;
                continue;
            }
            messageCount++;
            messageQueue.add(inputLine);
        }
    }

    class Dequeuer implements Runnable {
        private int id;

        public Dequeuer(int id) {
            this.id = id;
        }

        public void run() {
            while (!Thread.interrupted()) {
                try {
                    String message = messageQueue.take();
                    JSONObject jsonObject = new JSONObject(message);

                    // ignore delete records for now
                    if (jsonObject.has("delete")) {
                        continue;
                    }

                    Status status = getStatus(jsonObject);

                    EventWrapper ew = new EventWrapper(streamName, status, null);
                    for (io.s4.listener.EventHandler handler : handlers) {
                        try {
                            handler.processEvent(ew);
                        } catch (Exception e) {
                            Logger.getLogger("s4")
                                  .error("Exception in raw event handler", e);
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    Logger.getLogger("s4")
                          .error("Exception processing message", e);
                }
            }
        }

        public Status getStatus(JSONObject jsonObject) {
            try {
                if (jsonObject == null || jsonObject.equals(JSONObject.NULL)) {
                    return null;
                }

                Status status = new Status();

                status.setUser(getUser((JSONObject) jsonObject.opt("user")));

                Object value = jsonObject.opt("id");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    status.setId(((Number) value).longValue());
                }

                value = jsonObject.opt("in_reply_to_status_id");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    status.setInReplyToStatusId(((Number) value).longValue());
                }

                value = jsonObject.opt("text");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    status.setText((String) value);
                }

                value = jsonObject.opt("truncated");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    status.setTruncated((Boolean) value);
                }

                value = jsonObject.opt("source");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    status.setSource((String) value);
                }

                value = jsonObject.opt("in_reply_to_screen_name");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    status.setInReplyToScreenName((String) value);
                }

                value = jsonObject.opt("favorited");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    status.setFavorited((Boolean) value);
                }

                value = jsonObject.opt("in_reply_to_user_id");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    status.setInReplyToUserId(((Number) value).longValue());
                }

                value = jsonObject.opt("created_at");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    status.setCreatedAt((String) value);
                }

                return status;
            } catch (Exception e) {
                Logger.getLogger("s4").error(e);
            }

            return null;
        }

        public User getUser(JSONObject jsonObject) {
            try {
                if (jsonObject == null || jsonObject.equals(JSONObject.NULL)) {
                    return null;
                }

                User user = new User();

                Object value = jsonObject.opt("id");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setId(((Number) value).longValue());
                }

                value = jsonObject.opt("screen_name");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setScreenName((String) value);
                }

                value = jsonObject.opt("name");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setName((String) value);
                }

                value = jsonObject.opt("url");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setUrl((String) value);
                }

                value = jsonObject.opt("followers_count");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setFollowersCount(((Number) value).intValue());
                }

                value = jsonObject.opt("lang");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setLang((String) value);
                }

                value = jsonObject.opt("verified");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setVerified((Boolean) value);
                }

                value = jsonObject.opt("profile_image_url");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setProfileImageUrl((String) value);
                }

                value = jsonObject.opt("friends_count");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setFriendsCount(((Number) value).intValue());
                }

                value = jsonObject.opt("description");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setDescription((String) value);
                }

                value = jsonObject.opt("favourites_Count");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setFavouritesCount(((Number) value).intValue());
                }

                value = jsonObject.opt("geo_enabled");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setGeoEnabled((Boolean) value);
                }

                value = jsonObject.opt("listed_count");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setListedCount(((Number) value).intValue());
                }

                value = jsonObject.opt("profile_background_image_url");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setProfileBackgroundImageUrl((String) value);
                }

                value = jsonObject.opt("protected_user");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setProtectedUser((Boolean) value);
                }

                value = jsonObject.opt("location");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setLocation((String) value);
                }

                value = jsonObject.opt("statuses_count");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setStatusesCount(((Number) value).longValue());
                }

                value = jsonObject.opt("time_zone");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setTimeZone((String) value);
                }

                value = jsonObject.opt("contributors_enabled");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setContributorsEnabled((Boolean) value);
                }

                value = jsonObject.opt("utc_offset");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setUtcOffset(((Number) value).intValue());
                }

                value = jsonObject.opt("created_at");
                if (value != null && !value.equals(JSONObject.NULL)) {
                    user.setCreatedAt((String) value);
                }

                return user;
            } catch (Exception e) {
                Logger.getLogger("s4").error(e);
            }

            return null;
        }
    }

    @Override
    public void addHandler(EventHandler handler) {
        handlers.add(handler);

    }

    @Override
    public boolean removeHandler(EventHandler handler) {
        return handlers.remove(handler);
    }

}
