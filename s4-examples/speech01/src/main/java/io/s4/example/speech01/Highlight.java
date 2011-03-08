package io.s4.example.speech01;

public class Highlight {
    private long sentenceId;
    private long time;
    
    public long getSentenceId() {
        return sentenceId;
    }

    public void setSentenceId(long sentenceId) {
        this.sentenceId = sentenceId;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{sentenceId:")
            .append(sentenceId)
            .append(",time:")
            .append(time)
            .append("}");
        return sb.toString();
    }
}
