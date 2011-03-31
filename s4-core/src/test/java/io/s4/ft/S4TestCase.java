package io.s4.ft;

import io.s4.adapter.Adapter;
import io.s4.listener.EventProducer;
import io.s4.processor.PEContainer;
import io.s4.processor.ProcessingElement;
import io.s4.util.Watcher;
import io.s4.util.clock.Clock;
import io.s4.util.clock.EventClock;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.TestCase;

import org.junit.BeforeClass;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

public class S4TestCase extends TestCase {


    String configType = "typical";
    long seedTime = 0;
    String coreHome = "/Users/matthieu/";
    ApplicationContext appContext = null;
    ApplicationContext adapterContext = null;
    private String configBase;
    boolean configPathsInitialized = false;
    private String[] coreConfigFileUrls;

    // use a static map to track PE instances
    public static final Map<Object, ProcessingElement> registeredPEs = new Hashtable<Object, ProcessingElement>();
    public static String lockDirPath = System.getProperty("user.dir")
            + File.separator + "tmp" + File.separator + "lock";

    @BeforeClass
    public static void initS4Parameters() throws IOException {
        System.setProperty("commlayer_mode", "static");
        System.setProperty("commlayer.mode", "static");
        System.setProperty("DequeueCount", "6");
        System.setProperty("lock_dir", lockDirPath);
        File lockDir = new File(lockDirPath);
        if (!lockDir.exists()) {
            lockDir.mkdirs();
        } else {
            TestUtils.deleteDirectoryContents(lockDir);
        }

    }

    public void initConfigPaths(Class testClass) throws IOException {
        if (!configPathsInitialized) {
            initS4Parameters();
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
                System.exit(1);
            }

            configBase = System.getProperty("user.dir") + File.separator
                    + "src" + File.separator + "test" + File.separator + "java"
                    + File.separator + testDir + File.separator;
            String configPath = configBase + File.separatorChar
                    + "wall_clock.xml";
            List<String> coreConfigUrls = new ArrayList<String>();
            coreConfigUrls.add(configPath);

            // load core config xml
            configPath = configBase + "s4_core_conf.xml";
            File configFile = new File(configPath);
            if (!configFile.exists()) {
                System.err.printf("S4 core config file %s does not exist\n",
                        configPath);
                System.exit(1);
            }

            coreConfigUrls.add(configPath);
            String[] coreConfigFiles = new String[coreConfigUrls.size()];
            coreConfigUrls.toArray(coreConfigFiles);

            coreConfigFileUrls = new String[coreConfigFiles.length];
            for (int i = 0; i < coreConfigFiles.length; i++) {
                coreConfigFileUrls[i] = "file:" + coreConfigFiles[i];
            }
            configPathsInitialized = true;

        }
    }

    public void initializeS4App(Class testClass) throws Exception {
        initConfigPaths(testClass);
        ApplicationContext coreContext = null;

        coreContext = new FileSystemXmlApplicationContext(coreConfigFileUrls,
                coreContext);
        ApplicationContext context = coreContext;

        Clock s4Clock = (Clock) context.getBean("clock");
        if (s4Clock instanceof EventClock && seedTime > 0) {
            EventClock s4EventClock = (EventClock) s4Clock;
            s4EventClock.updateTime(seedTime);
            System.out.println("Intializing event clock time with seed time "
                    + s4EventClock.getCurrentTime());
        }

        PEContainer peContainer = (PEContainer) context.getBean("peContainer");

        Watcher w = (Watcher) context.getBean("watcher");
        w.setConfigFilename(configBase + "s4_core_conf.xml");

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
                .getBeanNamesForType(ProcessingElement.class);
        for (String processingElementBeanName : processingElementBeanNames) {
            Object bean = context.getBean(processingElementBeanName);
            try {
                Method getS4ClockMethod = bean.getClass().getMethod(
                        "getS4Clock");

                if (getS4ClockMethod.getReturnType().equals(Clock.class)) {
                    if (getS4ClockMethod.invoke(bean) == null) {
                        Method setS4ClockMethod = bean.getClass().getMethod(
                                "setS4Clock", Clock.class);
                        setS4ClockMethod.invoke(bean,
                                coreContext.getBean("clock"));
                    }
                }
            } catch (NoSuchMethodException mnfe) {
                // acceptable
            }
            System.out.println("Adding processing element with bean name "
                    + processingElementBeanName + ", id "
                    + ((ProcessingElement) bean).getId());
            peContainer.addProcessor((ProcessingElement) bean);
        }

        appContext = context;

    }

    public void initializeAdapter() throws IOException {
        initConfigPaths(getClass());
        // load adapter config xml

        // load adapter config xml
        ApplicationContext coreContext;
        coreContext = new FileSystemXmlApplicationContext("file:" + configBase
                + "adapter_conf.xml");
        ApplicationContext context = coreContext;

        adapterContext = new FileSystemXmlApplicationContext(
                new String[] { "file:" + configBase + "app_adapter_conf.xml" },
                context);

        Adapter adapter = (Adapter) adapterContext.getBean("adapter");

        Map listenerBeanMap = adapterContext
                .getBeansOfType(EventProducer.class);
        if (listenerBeanMap.size() == 0) {
            System.err.println("No user-defined listener beans");
            System.exit(1);
        }
        EventProducer[] eventListeners = new EventProducer[listenerBeanMap
                .size()];

        int index = 0;
        for (Iterator it = listenerBeanMap.keySet().iterator(); it.hasNext(); index++) {
            String beanName = (String) it.next();
            System.out.println("Adding producer " + beanName);
            eventListeners[index] = (EventProducer) listenerBeanMap
                    .get(beanName);
        }

        adapter.setEventListeners(eventListeners);

    }

}
