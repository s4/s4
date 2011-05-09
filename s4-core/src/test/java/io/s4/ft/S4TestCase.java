package io.s4.ft;

import io.s4.processor.ProcessingElement;

import java.io.File;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

import org.junit.BeforeClass;
import org.springframework.context.ApplicationContext;

public class S4TestCase {

    String configType = "typical";
    long seedTime = 0;
    ApplicationContext appContext = null;
    ApplicationContext adapterContext = null;
    private String configBase;
    boolean configPathsInitialized = false;
    private String[] coreConfigFileUrls;
    public static File DEFAULT_TEST_OUTPUT_DIR = new File(
            System.getProperty("user.dir") + File.separator + "tmp");
    public static File DEFAULT_STORAGE_DIR = new File(
            DEFAULT_TEST_OUTPUT_DIR.getAbsolutePath() + File.separator
                    + "storage");
    // use a static map to track PE instances
    public static final Map<Object, ProcessingElement> registeredPEs = new Hashtable<Object, ProcessingElement>();

    
    @BeforeClass
    public static void cleanLocks() {
        TestUtils.cleanupTmpDirs();
    }
    

    @BeforeClass
    public static void initS4Parameters() throws IOException {
    
        System.setProperty("commlayer_mode", "static");
        System.setProperty("commlayer.mode", "static");
        System.setProperty("DequeueCount", "6");
        System.setProperty("lock_dir", S4App.lockDirPath);
        File lockDir = new File(S4App.lockDirPath);
        if (!lockDir.exists()) {
            lockDir.mkdirs();
        } else {
            TestUtils.deleteDirectoryContents(lockDir);
        }
    }

    
    
//    public void initializeAdapter() throws IOException {
//        initConfigPaths(getClass(), null);
//        // load adapter config xml
//
//        // load adapter config xml
//        ApplicationContext coreContext;
//        coreContext = new FileSystemXmlApplicationContext("file:" + configBase
//                + "adapter_conf.xml");
//        ApplicationContext context = coreContext;
//
//        adapterContext = new FileSystemXmlApplicationContext(
//                new String[] { "file:" + configBase + "app_adapter_conf.xml" },
//                context);
//
//        Adapter adapter = (Adapter) adapterContext.getBean("adapter");
//
//        Map listenerBeanMap = adapterContext
//                .getBeansOfType(EventProducer.class);
//        if (listenerBeanMap.size() == 0) {
//            System.err.println("No user-defined listener beans");
//            Thread.dumpStack();
//            System.exit(14);
//        }
//        EventProducer[] eventListeners = new EventProducer[listenerBeanMap
//                .size()];
//
//        int index = 0;
//        for (Iterator it = listenerBeanMap.keySet().iterator(); it.hasNext(); index++) {
//            String beanName = (String) it.next();
//            System.out.println("Adding producer " + beanName);
//            eventListeners[index] = (EventProducer) listenerBeanMap
//                    .get(beanName);
//        }
//
//        adapter.setEventListeners(eventListeners);
//
//    }

}
