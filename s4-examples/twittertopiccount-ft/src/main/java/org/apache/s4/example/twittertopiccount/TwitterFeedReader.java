package org.apache.s4.example.twittertopiccount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

/**
 * 
 * Adapter for injecting twitter data from a twitter dump file, rather than from a live http stream.
 * 
 * The twitter dumps must be located in a directory in the file system
 *
 */
public class TwitterFeedReader extends TwitterFeedListener {

    String frequencyBySecond;
    String twitterDumpsDir;
    String twitterDumpsNamePattern = "\\A.+\\.gz\\z";

    @Override
    public void connectAndRead() throws Exception {
        System.out.println("Reading files from dir " +  twitterDumpsDir + " matching: " + twitterDumpsNamePattern);
        File[] dumps = new File(twitterDumpsDir)
                .listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.matches(twitterDumpsNamePattern);
                    }
                });
        for (File dump : dumps) {
            System.out.println("Reading file : " + dump.getAbsolutePath());
            GZIPInputStream gzipIs = new GZIPInputStream(new FileInputStream(
                    dump));
            InputStreamReader isr = new InputStreamReader(gzipIs);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null) {
                // only consider lines with twitter json-encoded data
                if (line.startsWith("{")) {
//                    System.out.println("Adding line : " + line);
                    messageQueue.add(line);
                    Thread.sleep((1000 / Integer.valueOf(frequencyBySecond)));
                }
            }
            br.close();
        }
        System.out.println("OK, read all dump files. Exiting normally.");
        System.exit(0);
    }

    public void setFrequencyBySecond(String frequencyBySecond) {
        this.frequencyBySecond = frequencyBySecond;
    }

    public void setTwitterDumpsDir(String twitterDumpsDir) {
        this.twitterDumpsDir = twitterDumpsDir;
    }

    public void setTwitterDumpsNamePattern(String twitterDumpsNamePattern) {
        this.twitterDumpsNamePattern = twitterDumpsNamePattern;
    }

}
