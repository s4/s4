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

public class User {
    private long id;
    private String screenName;
    private String name;
    private String url;
    private int followersCount;
    private String lang;
    private boolean verified;
    private String profileImageUrl;
    private int friendsCount;
    private String description;
    private int favouritesCount;
    private boolean geoEnabled;
    private int listedCount;
    private String profileBackgroundImageUrl;
    private boolean protectedUser;
    private String location;
    private long statusesCount;
    private String timeZone;
    private boolean contributorsEnabled;
    private int utcOffset;
    private String createdAt;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getScreenName() {
        return screenName;
    }

    public void setScreenName(String screenName) {
        this.screenName = screenName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getFollowersCount() {
        return followersCount;
    }

    public void setFollowersCount(int followersCount) {
        this.followersCount = followersCount;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public boolean isVerified() {
        return verified;
    }

    public void setVerified(boolean verified) {
        this.verified = verified;
    }

    public String getProfileImageUrl() {
        return profileImageUrl;
    }

    public void setProfileImageUrl(String profileImageUrl) {
        this.profileImageUrl = profileImageUrl;
    }

    public int getFriendsCount() {
        return friendsCount;
    }

    public void setFriendsCount(int friendsCount) {
        this.friendsCount = friendsCount;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getFavouritesCount() {
        return favouritesCount;
    }

    public void setFavouritesCount(int favouritesCount) {
        this.favouritesCount = favouritesCount;
    }

    public boolean isGeoEnabled() {
        return geoEnabled;
    }

    public void setGeoEnabled(boolean geoEnabled) {
        this.geoEnabled = geoEnabled;
    }

    public int getListedCount() {
        return listedCount;
    }

    public void setListedCount(int listedCount) {
        this.listedCount = listedCount;
    }

    public String getProfileBackgroundImageUrl() {
        return profileBackgroundImageUrl;
    }

    public void setProfileBackgroundImageUrl(String profileBackgroundImageUrl) {
        this.profileBackgroundImageUrl = profileBackgroundImageUrl;
    }

    public boolean isProtectedUser() {
        return protectedUser;
    }

    public void setProtectedUser(boolean protectedUser) {
        this.protectedUser = protectedUser;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public long getStatusesCount() {
        return statusesCount;
    }

    public void setStatusesCount(long statusesCount) {
        this.statusesCount = statusesCount;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public boolean isContributorsEnabled() {
        return contributorsEnabled;
    }

    public void setContributorsEnabled(boolean contributorsEnabled) {
        this.contributorsEnabled = contributorsEnabled;
    }

    public int getUtcOffset() {
        return utcOffset;
    }

    public void setUtcOffset(int utcOffset) {
        this.utcOffset = utcOffset;
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
          .append("screenName:")
          .append(screenName)
          .append(",")
          .append("name:")
          .append(name)
          .append(",")
          .append("url:")
          .append(url)
          .append(",")
          .append("followersCount:")
          .append(followersCount)
          .append(",")
          .append("lang:")
          .append(lang)
          .append(",")
          .append("verified:")
          .append(verified)
          .append(",")
          .append("profileImageUrl:")
          .append(profileImageUrl)
          .append(",")
          .append("friendsCount:")
          .append(friendsCount)
          .append(",")
          .append("description:")
          .append(description)
          .append(",")
          .append("favouritesCount:")
          .append(favouritesCount)
          .append(",")
          .append("geoEnabled:")
          .append(geoEnabled)
          .append(",")
          .append("listedCount:")
          .append(listedCount)
          .append(",")
          .append("profileBackgroundImageUrl:")
          .append(profileBackgroundImageUrl)
          .append(",")
          .append("protectedUser:")
          .append(protectedUser)
          .append(",")
          .append("location:")
          .append(location)
          .append(",")
          .append("statusesCount:")
          .append(statusesCount)
          .append(",")
          .append("timeZone:")
          .append(timeZone)
          .append(",")
          .append("contributorsEnabled:")
          .append(contributorsEnabled)
          .append(",")
          .append("utcOffset:")
          .append(utcOffset)
          .append(",")
          .append("createdAt:")
          .append(createdAt)
          .append("}");

        return sb.toString();

    }

}
