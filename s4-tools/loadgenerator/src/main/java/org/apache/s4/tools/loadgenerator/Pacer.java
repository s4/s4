/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *          http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package org.apache.s4.tools.loadgenerator;;

public class Pacer {
    private long sleepOverheadMicros = -1;
    private int expectedRate = -1;
    private int adjustedExpectedRate = 1;
    private long startTime;
    private int cycleCount = 0;

    private static int PROCESS_TIME_LIST_MAX_SIZE = 15;
    private long[] processTimes = new long[PROCESS_TIME_LIST_MAX_SIZE];
    private int processTimePointer = 0;
    private long[] rateInfo = new long[] {0,100};
    
    public Pacer(int expectedRate) {
        this.expectedRate = expectedRate;
        this.adjustedExpectedRate = expectedRate; // the same for now

        if (sleepOverheadMicros == -1) {
            // calculate sleep overhead
            long totalSleepOverhead = 0;
            for (int i = 0; i < 50; i++) {
                long startTime = System.nanoTime();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ie) {
                }
                totalSleepOverhead += (System.nanoTime() - startTime)
                        - (1 * 1000 * 1000);
            }
            sleepOverheadMicros = (totalSleepOverhead / 50) / 1000;
        }
    }
    
    public void startCycle() {
        startTime = System.nanoTime();
    }
    
    public void endCycle() {
        processTimes[processTimePointer] = System.nanoTime() - startTime;
        processTimePointer = (processTimePointer == PROCESS_TIME_LIST_MAX_SIZE - 1) ? 0
                : processTimePointer + 1;
        
        cycleCount++;
        
    }
    
    public void maintainPace() {   
        if (cycleCount == 1 || cycleCount % 20 == 0) {
            rateInfo = getRateInfo(rateInfo);
        }  
        if (rateInfo[1] == 0 || cycleCount % rateInfo[1] == 0) {
            try {
                Thread.sleep(rateInfo[0]);
            } catch (InterruptedException ie) {
            }
        }
    }
    
    private long[] getRateInfo(long[] rateInfo) {
        long totalTimeNanos = 0;
        int entryCount = 0;
        for (int i = 0; i < processTimes.length; i++) {
            if (processTimes[i] == Long.MIN_VALUE) {
                break;
            }
            entryCount++;
            totalTimeNanos += processTimes[i];
        }
        long averageTimeMicros = (long) ((totalTimeNanos / (double) entryCount) / 1000.0);
        // fudge the time for additional overhead
        averageTimeMicros += (long) (averageTimeMicros * 0.30);

        if (cycleCount % 5000 == 0) {
            // System.out.println("Average time in micros is " +
            // averageTimeMicros);
        }

        long sleepTimeMicros = 0;
        long millis = 0;

        long timeToMeetRateMicros = adjustedExpectedRate * averageTimeMicros;
        long leftOver = 1000000 - timeToMeetRateMicros;
        if (leftOver <= 0) {
            sleepTimeMicros = 0;
        } else {
            sleepTimeMicros = (leftOver / adjustedExpectedRate)
                    - sleepOverheadMicros;
        }

        // how many events can be processed in the nanos time?
        int eventsBeforeSleep = 1;
        if (sleepTimeMicros < 1000) {
            // less than 1 millisecond sleep time, so need to stagger sleeps to
            // emulate such a sleep
            sleepTimeMicros = 1000 + sleepOverheadMicros;
            millis = 1;
            double numNapsDouble = ((double) leftOver / sleepTimeMicros);
            int numNaps = (int) Math.ceil(numNapsDouble);
            if (numNaps > 0) {
                eventsBeforeSleep = adjustedExpectedRate / numNaps;
            }

            if (leftOver <= 0) {
                millis = 0;
                eventsBeforeSleep = 1000;
            }
        } else {
            millis = sleepTimeMicros / 1000;
        }

        rateInfo[0] = millis;
        rateInfo[1] = eventsBeforeSleep;
        return rateInfo;
    }
}
