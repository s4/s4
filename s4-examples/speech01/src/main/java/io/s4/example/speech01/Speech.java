package io.s4.example.speech01;

public class Speech {
    private long id;
    private String location;
    private String speaker;
    private long time;
    
    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getSpeaker() {
        return speaker;
    }

    public void setSpeaker(String speaker) {
        this.speaker = speaker;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("{id:")
            .append(id)
            .append(",location:")
            .append(location)
            .append(",speaker")
            .append(speaker)
            .append(",time")
            .append(time)
            .append("}");
        
        return sb.toString();
    }
        
}
