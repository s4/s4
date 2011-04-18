package io.s4.wordcount;

import java.util.ArrayList;
import java.util.List;

import io.s4.dispatcher.EventDispatcher;
import io.s4.ft.KeyValue;
import io.s4.processor.AbstractPE;

public class WordSplitter extends AbstractPE {

    private transient EventDispatcher dispatcher;
    private String outputStreamName;
    private String id;

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

    @Override
    public String getId() {
        return id;
    }

    public void processEvent(KeyValue keyValue) {
        List<List<String>> compoundKeyNames = new ArrayList<List<String>>();
        List<String> keyNames = new ArrayList<String>(1);
        keyNames.add("word");
        compoundKeyNames.add(keyNames);
        if ("sentence".equals(keyValue.getKey())) {
            String[] split = keyValue.getValue().split(" ");
            for (int i = 0; i < split.length; i++) {
                dispatcher.dispatchEvent(outputStreamName, compoundKeyNames,
                        new Word(split[i]));
            }
        }
    }

    @Override
    public void output() {
        // TODO Auto-generated method stub

    }

}
