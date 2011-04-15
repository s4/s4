package io.s4.wordcount;

import io.s4.dispatcher.EventDispatcher;
import io.s4.ft.KeyValue;
import io.s4.processor.AbstractPE;

public class WordCounter extends AbstractPE {

    private transient EventDispatcher dispatcher;
    private String outputStreamName;
    int wordCounter;
    private String id;

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

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

    public void processEvent(Word word) {
        wordCounter++;
        // System.out.println("seen word " + word.getWord());
    }


    @Override
    public void output() {
        System.out.println("dispatch: " + getKeyValueString() + " / "
                + wordCounter);
        dispatcher.dispatchEvent(outputStreamName, null, new WordCount(
                getKeyValueString(), wordCounter));
    }


}
