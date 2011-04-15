package io.s4.wordcount;

public class WordCount {

    public String word;
    public int count;

    public WordCount() {
    }

    public WordCount(String word, int count) {
        super();
        this.word = word;
        this.count = count;
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

}
