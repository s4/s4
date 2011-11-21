package org.apache.s4.ft;

import org.apache.s4.processor.AbstractPE;

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
    boolean configPathsInitialized = false;
    public static File DEFAULT_TEST_OUTPUT_DIR = new File(
            System.getProperty("user.dir") + File.separator + "tmp");
    public static File DEFAULT_STORAGE_DIR = new File(
            DEFAULT_TEST_OUTPUT_DIR.getAbsolutePath() + File.separator
                    + "storage");
    // use a static map to track PE instances
    public static final Map<Object, AbstractPE> registeredPEs = new Hashtable<Object, AbstractPE>();

    
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
            if (!lockDir.mkdirs()) {
                throw new RuntimeException("Cannot create directory: ["+lockDir.getAbsolutePath()+"]");
            }
        } else {
            TestUtils.deleteDirectoryContents(lockDir);
        }
    }

}
