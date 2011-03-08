package io.s4.example.speech01;

import io.s4.processor.AbstractPE;

public class SentenceReceiverPE extends AbstractPE {

    public void processEvent(Sentence sentence) {
        System.out.printf("Sentence is '%s', location %s\n", sentence.getText(), sentence.getLocation());
    }
    
    @Override
    public void output() {
        // not called in this example
    }

    @Override
    public String getId() {
        return this.getClass().getName();
    }

}
