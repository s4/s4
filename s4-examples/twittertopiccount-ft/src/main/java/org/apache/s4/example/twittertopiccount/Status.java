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
package org.apache.s4.example.twittertopiccount;

public class Status {
    private long id;
    private long inReplyToStatusId;
    private String text;
    private boolean truncated;
    private String source;
    private String inReplyToScreenName;
    private boolean favorited;
    private User user;
    private long inReplyToUserId;
    private String createdAt;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getInReplyToStatusId() {
        return inReplyToStatusId;
    }

    public void setInReplyToStatusId(long inReplyToStatusId) {
        this.inReplyToStatusId = inReplyToStatusId;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public boolean isTruncated() {
        return truncated;
    }

    public void setTruncated(boolean truncated) {
        this.truncated = truncated;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getInReplyToScreenName() {
        return inReplyToScreenName;
    }

    public void setInReplyToScreenName(String inReplyToScreenName) {
        this.inReplyToScreenName = inReplyToScreenName;
    }

    public boolean isFavorited() {
        return favorited;
    }

    public void setFavorited(boolean favorited) {
        this.favorited = favorited;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public long getInReplyToUserId() {
        return inReplyToUserId;
    }

    public void setInReplyToUserId(long inReplyToUserId) {
        this.inReplyToUserId = inReplyToUserId;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("{")
          .append("id:")
          .append(id)
          .append(",")
          .append("inReplyToStatusId:")
          .append(inReplyToStatusId)
          .append(",")
          .append("text:")
          .append(text)
          .append(",")
          .append("truncated:")
          .append(truncated)
          .append(",")
          .append("source:")
          .append(source)
          .append(",")
          .append("inReplyToScreenName:")
          .append(inReplyToScreenName)
          .append(",")
          .append("favorited:")
          .append(favorited)
          .append(",")
          .append("user:")
          .append(user)
          .append(",")
          .append("inReplyToUserId:")
          .append(inReplyToUserId)
          .append(",")
          .append("createdAt:")
          .append(createdAt)
          .append("}");

        return sb.toString();
    }

    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException cnse) {
            throw new RuntimeException(cnse);
        }
    }
}
