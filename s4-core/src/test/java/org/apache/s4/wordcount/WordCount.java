package org.apache.s4.wordcount;

public class WordCount {

    private String word;
    private int count;
    private String routingKey;

    public WordCount() {
    }

    public WordCount(String word, int count, String routingKey) {
        super();
        this.word = word;
        this.count = count;
        this.routingKey = routingKey;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

}
