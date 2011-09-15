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
package io.s4.tools.loadgenerator;

import io.s4.client.Driver;
import io.s4.client.Message;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.JSONException;
import org.json.JSONObject;

public class LoadGenerator {

    public static void main(String args[]) {
        Options options = new Options();
        boolean warmUp = false;

        options.addOption(OptionBuilder.withArgName("rate")
                                       .hasArg()
                                       .withDescription("Rate (events per second)")
                                       .create("r"));

        options.addOption(OptionBuilder.withArgName("display_rate")
                                       .hasArg()
                                       .withDescription("Display Rate at specified second boundary")
                                       .create("d"));

        options.addOption(OptionBuilder.withArgName("adapter_address")
                                       .hasArg()
                                       .withDescription("Address of client adapter")
                                       .create("a"));

        options.addOption(OptionBuilder.withArgName("listener_application_name")
                                       .hasArg()
                                       .withDescription("Listener application name")
                                       .create("g"));

        options.addOption(OptionBuilder.withArgName("sleep_overhead")
                                       .hasArg()
                                       .withDescription("Sleep overhead")
                                       .create("o"));

        options.addOption(new Option("w", "Warm-up"));

        CommandLineParser parser = new GnuParser();

        CommandLine line = null;
        try {
            // parse the command line arguments
            line = parser.parse(options, args);
        } catch (ParseException exp) {
            // oops, something went wrong
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            System.exit(1);
        }

        int expectedRate = 250;
        if (line.hasOption("r")) {
            try {
                expectedRate = Integer.parseInt(line.getOptionValue("r"));
            } catch (Exception e) {
                System.err.println("Bad expected rate specified "
                        + line.getOptionValue("r"));
                System.exit(1);
            }
        }

        int displayRateIntervalSeconds = 20;
        if (line.hasOption("d")) {
            try {
                displayRateIntervalSeconds = Integer.parseInt(line.getOptionValue("d"));
            } catch (Exception e) {
                System.err.println("Bad display rate value specified "
                        + line.getOptionValue("d"));
                System.exit(1);
            }
        }

        int updateFrequency = 0;
        if (line.hasOption("f")) {
            try {
                updateFrequency = Integer.parseInt(line.getOptionValue("f"));
            } catch (Exception e) {
                System.err.println("Bad query udpdate frequency specified "
                        + line.getOptionValue("f"));
                System.exit(1);
            }
            System.out.printf("Update frequency is %d\n", updateFrequency);
        }

        String clientAdapterAddress = null;
        String clientAdapterHost = null;
        int clientAdapterPort = -1;
        if (line.hasOption("a")) {
            clientAdapterAddress = line.getOptionValue("a");
            String[] parts = clientAdapterAddress.split(":");
            if (parts.length != 2) {
                System.err.println("Bad adapter address specified "
                        + clientAdapterAddress);
                System.exit(1);
            }
            clientAdapterHost = parts[0];
            
            try {
                clientAdapterPort = Integer.parseInt(parts[1]);
            }
            catch (NumberFormatException nfe) {
                System.err.println("Bad adapter address specified "
                        + clientAdapterAddress);
                System.exit(1);                
            }
        }

        long sleepOverheadMicros = -1;
        if (line.hasOption("o")) {
            try {
                sleepOverheadMicros = Long.parseLong(line.getOptionValue("o"));
            } catch (NumberFormatException e) {
                System.err.println("Bad sleep overhead specified "
                        + line.getOptionValue("o"));
                System.exit(1);
            }
            System.out.printf("Specified sleep overhead is %d\n",
                              sleepOverheadMicros);
        }

        if (line.hasOption("w")) {
            warmUp = true;
        }

        List loArgs = line.getArgList();
        if (loArgs.size() < 1) {
            System.err.println("No input file specified");
            System.exit(1);
        }

        String inputFilename = (String) loArgs.get(0);

        LoadGenerator loadGenerator = new LoadGenerator();
        loadGenerator.setInputFilename(inputFilename);
        loadGenerator.setDisplayRateInterval(displayRateIntervalSeconds);
        loadGenerator.setExpectedRate(expectedRate);
        loadGenerator.setClientAdapterHost(clientAdapterHost);
        loadGenerator.setClientAdapterPort(clientAdapterPort);
        loadGenerator.run();

        System.exit(0);
    }

    private String inputFilename;
    private int emitCount;
    private int displayRateInterval = 0;
    private int expectedRate = 200;
    private String clientAdapterHost = null;
    private int clientAdapterPort = -1;
    
    private int adjustedExpectedRate = 1;
    private Map<Integer, EventTypeInfo> eventTypeInfoMap = new HashMap<Integer, EventTypeInfo>();
    private Driver driver;
    private boolean isConnected;

    public int getEmitCount() {
        return emitCount;
    }

    public void setInputFilename(String inputFilename) {
        this.inputFilename = inputFilename;
    }

    public void setDisplayRateInterval(int displayRateInterval) {
        this.displayRateInterval = displayRateInterval;
    }

    public void setExpectedRate(int expectedRate) {
        this.expectedRate = expectedRate;
    }

    public void setClientAdapterHost(String clientAdapterHost) {
        this.clientAdapterHost = clientAdapterHost;
    }

    public void setClientAdapterPort(int clientAdapterPort) {
        this.clientAdapterPort = clientAdapterPort;
    }

    public LoadGenerator() {
        
    }

    public void run() {
        // for now, no warm-up mechanism
        adjustedExpectedRate = expectedRate;

        long intervalStart = 0;
        int emitCountStart = 0;

        BufferedReader br = null;
        Reader inputReader = null;
        try {
            if (!connect()) {
                System.err.println("Failed to initialize client adapter driver");
                return;
            }
            
            if (inputFilename.equals("-")) {
                inputReader = new InputStreamReader(System.in);
            } else {
                inputReader = new FileReader(inputFilename);
            }
            br = new BufferedReader(inputReader);
            String inputLine = null;
            boolean firstLine = true;
            
            Pacer pacer = new Pacer(adjustedExpectedRate);
            while ((inputLine = br.readLine()) != null) {
                if (firstLine) {
                    JSONObject jsonRecord = new JSONObject(inputLine);
                    createEventTypeInfo(jsonRecord);
                    System.out.println(eventTypeInfoMap);
                    if (eventTypeInfoMap.size() == 0) {
                        return;
                    }
                    firstLine = false;
                    continue;
                }
                
                pacer.startCycle();

                try {
                    JSONObject jsonRecord = new JSONObject(inputLine);
                    int classIndex = jsonRecord.getInt("_index");
                    EventTypeInfo eventTypeInfo = eventTypeInfoMap.get(classIndex);
                      
                    if (eventTypeInfo == null) {
                        System.err.printf("Invalid _index value %d\n",
                                          classIndex);
                        return;
                    }
                    
                    Message message = new Message(eventTypeInfo.getStreamName(), eventTypeInfo.getClassName(), inputLine);
                    sendMessage(message);
                    emitCount++;
                } catch (JSONException je) {
                    je.printStackTrace();
                    System.err.printf("Bad input data %s\n", inputLine);
                    continue;
                }

                // if it's time, display the actual emit rate
                if (intervalStart == 0) {
                    intervalStart = System.currentTimeMillis();
                } else {
                    long interval = System.currentTimeMillis() - intervalStart;
                    if (interval >= (displayRateInterval * 1000)) {
                        double rate = (emitCount - emitCountStart)
                                / (interval / 1000.0);
                        System.out.println("Rate is " + rate);
                        intervalStart = System.currentTimeMillis();
                        emitCountStart = emitCount;
                    }
                }

                pacer.endCycle();
                pacer.maintainPace();
            }
            System.out.printf("Emitted %d events\n", emitCount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                br.close();
            } catch (Exception e) {
            }
            try {
                inputReader.close();
            } catch (Exception e) {
            }
            try {
                driver.disconnect();
            } catch (Exception e) {
            }
        }
    }
    
    public boolean connect() {
        isConnected = false;
        try {
            System.out.println("Connecting...");
            driver = new Driver(clientAdapterHost, clientAdapterPort);
            boolean isInitialized = driver.init();
            isConnected = isInitialized & driver.connect();
            System.out.println("Connection made: " + isConnected);
            return isConnected;
        }
        catch (IOException ioe) {
            System.out.println("Connection made: " + isConnected);
            return isConnected;
        }
        catch (NullPointerException npe) {
            // there's a bug in the driver that causes a null pointer exception if
            // if the target server is down
            System.out.println("Connection made: " + isConnected);
            return isConnected;
        }
    }
    
    public boolean sendMessage(Message message) {
        final int MAX_RETRY = 5;
        boolean sent = false;
        int backoff = 10;
        for (int retries = 0; retries < MAX_RETRY; retries++) {
            try {
                if (!isConnected) {
                    throw new IOException("Driver not connected");
                }
                driver.send(message);
                sent = true;
                break;
            }
            catch (IOException ioe) {
                try {
                    System.out.printf("Sleeping for %f seconds\n", backoff/1000.0);
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {}
                backoff = backoff*5;
                connect();
            }
        }
        return sent;
    }

    @SuppressWarnings("unchecked")
    public void createEventTypeInfo(JSONObject classInfo) {
        String className = "";
        try {
            for (Iterator it = classInfo.keys(); it.hasNext();) {
                className = (String) it.next();
                JSONObject jsonEventTypeInfo = classInfo.getJSONObject(className);
                int classIndex = (Integer) jsonEventTypeInfo.getInt("classIndex");
                String streamName = jsonEventTypeInfo.getString("streamName");
                eventTypeInfoMap.put(classIndex, new EventTypeInfo(className,
                                                                   streamName));
            }
        } catch (JSONException je) {
            je.printStackTrace();
        }
    }

    static class EventTypeInfo {
        private String className;
        private String streamName;

        public EventTypeInfo(String clazz, String streamName) {
            this.className = clazz;
            this.streamName = streamName;
        }

        public String getClassName() {
            return className;
        }

        public String getStreamName() {
            return streamName;
        }
    }
}
