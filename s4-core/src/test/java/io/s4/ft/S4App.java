package io.s4.ft;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;

public class S4App extends S4TestCase {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Class testClass = Class.forName(args[0]);
        String s4CoreConfFile = args[1];
        S4App app = new S4App();
        initS4Parameters();
        app.initializeS4App(testClass, s4CoreConfFile);

    }

}
