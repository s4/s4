package org.apache.s4.ft;

import org.apache.s4.processor.AbstractPE;
import org.apache.s4.processor.PEContainer;
import org.apache.s4.util.Watcher;
import org.apache.s4.util.clock.Clock;
import org.apache.s4.util.clock.EventClock;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

/**
 * 
 *
 */
public class S4App {

    String configType = "typical";
    long seedTime = 0;
    ApplicationContext appContext = null;
    ApplicationContext adapterContext = null;
    private String configBase;
    boolean configPathsInitialized = false;
    private String[] coreConfigFileUrls;
    private Class testClass;
    private String s4CoreConfFileName;
    public static File DEFAULT_TEST_OUTPUT_DIR = new File(
            System.getProperty("user.dir") + File.separator + "tmp");
    public static File DEFAULT_STORAGE_DIR = new File(
            DEFAULT_TEST_OUTPUT_DIR.getAbsolutePath() + File.separator
                    + "storage");

    public static String lockDirPath = System.getProperty("user.dir")
            + File.separator + "tmp" + File.separator + "lock";

    private S4App() {}
    
    public S4App(Class testClass, String s4CoreConfFileName) throws Exception {
        this.testClass = testClass;
        this.s4CoreConfFileName = s4CoreConfFileName;
        initConfigPaths(testClass, s4CoreConfFileName);
    }
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Class testClass = Class.forName(args[0]);
        String s4CoreConfFile = args[1];
        S4App app = new S4App(testClass, s4CoreConfFile);
        S4TestCase.initS4Parameters();
        app.initializeS4App();

    }

    /**
     * Performs dependency injection and starts the S4 plaftform.
     */
    public void initializeS4App()
            throws Exception {
        initConfigPaths(testClass, s4CoreConfFileName);
        ApplicationContext coreContext = null;

        coreContext = new FileSystemXmlApplicationContext(coreConfigFileUrls,
                coreContext);
        ApplicationContext context = coreContext;

        Clock clock = (Clock) context.getBean("clock");
        if (clock instanceof EventClock && seedTime > 0) {
            EventClock s4EventClock = (EventClock) clock;
            s4EventClock.updateTime(seedTime);
            System.out.println("Intializing event clock time with seed time "
                    + s4EventClock.getCurrentTime());
        }

        PEContainer peContainer = (PEContainer) context.getBean("peContainer");

        Watcher w = (Watcher) context.getBean("watcher");
        w.setConfigFilename(configBase + s4CoreConfFileName);

        // load extension modules
        // String[] configFileNames = getModuleConfigFiles(extsHome, prop);
        // if (configFileNames.length > 0) {
        // String[] configFileUrls = new String[configFileNames.length];
        // for (int i = 0; i < configFileNames.length; i++) {
        // configFileUrls[i] = "file:" + configFileNames[i];
        // }
        // context = new FileSystemXmlApplicationContext(configFileUrls,
        // context);
        // }

        // load application modules
        String applicationConfigFileName = configBase + "app_conf.xml";
        String[] configFileUrls = new String[] { "file:"
                + applicationConfigFileName };
        context = new FileSystemXmlApplicationContext(configFileUrls, context);
        // attach any beans that implement ProcessingElement to the PE
        // Container
        String[] processingElementBeanNames = context
                .getBeanNamesForType(AbstractPE.class);
        for (String processingElementBeanName : processingElementBeanNames) {
            Object bean = context.getBean(processingElementBeanName);
            try {
                Method getS4ClockMethod = bean.getClass().getMethod(
                        "getClock");

                if (getS4ClockMethod.getReturnType().equals(Clock.class)) {
                    if (getS4ClockMethod.invoke(bean) == null) {
                        Method setS4ClockMethod = bean.getClass().getMethod(
                                "setClock", Clock.class);
                        setS4ClockMethod.invoke(bean,
                                coreContext.getBean("clock"));
                    }
                }
                ((AbstractPE)bean).setSafeKeeper((SafeKeeper) context.getBean("safeKeeper"));
            } catch (NoSuchMethodException mnfe) {
                // acceptable
            }
            System.out.println("Adding processing element with bean name "
                    + processingElementBeanName + ", id "
                    + ((AbstractPE) bean).getId());
            peContainer.addProcessor((AbstractPE) bean);
        }

        appContext = context;
    }
    
    

    private void initConfigPaths(Class testClass, String s4CoreConfFileName)
            throws IOException {
        if (!configPathsInitialized) {
            S4TestCase.initS4Parameters();
            String testDir = testClass.getPackage().getName()
                    .replace('.', File.separatorChar);

            ClassPathResource propResource = new ClassPathResource(
                    "s4_core.properties");
            Properties prop = new Properties();
            if (propResource.exists()) {
                prop.load(propResource.getInputStream());
            } else {
                System.err
                        .println("Unable to find s4_core.properties. It must be available in classpath");
                Thread.dumpStack();
                System.exit(12);
            }

            configBase = System.getProperty("user.dir") + File.separator
                    + "src" + File.separator + "test" + File.separator + "java"
                    + File.separator + testDir + File.separator;
            String configPath = configBase + File.separatorChar
                    + "wall_clock.xml";
            List<String> coreConfigUrls = new ArrayList<String>();
            coreConfigUrls.add(configPath);

            // load core config xml
            if (s4CoreConfFileName != null) {
                // may be null for adapter
                configPath = configBase + s4CoreConfFileName;
                File configFile = new File(configPath);
                if (!configFile.exists()) {
                    System.err.printf(
                            "S4 core config file %s does not exist\n",
                            configPath);
                    Thread.dumpStack();
                    System.exit(13);
                }
                coreConfigUrls.add(configPath);
            }
            String[] coreConfigFiles = new String[coreConfigUrls.size()];
            coreConfigUrls.toArray(coreConfigFiles);

            coreConfigFileUrls = new String[coreConfigFiles.length];
            for (int i = 0; i < coreConfigFiles.length; i++) {
                coreConfigFileUrls[i] = "file:" + coreConfigFiles[i];
            }
            configPathsInitialized = true;

        }
    }
    
    public void destroy() {
        ((FileSystemXmlApplicationContext)appContext).close();
    }
}
