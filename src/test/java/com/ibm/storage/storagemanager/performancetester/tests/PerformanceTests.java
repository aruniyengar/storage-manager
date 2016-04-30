/**
 * 
 */
package com.ibm.storage.storagemanager.performancetester.tests;

import java.util.ArrayList;

import org.junit.Test;

import com.ibm.storage.storagemanager.implementations.cloudant.KeyValueCloudant;
import com.ibm.storage.storagemanager.implementations.file.KeyValueFile;
import com.ibm.storage.storagemanager.implementations.objectstorage.KeyValueObjectStorage;
import com.ibm.storage.storagemanager.implementations.redis.KeyValueRedis;
import com.ibm.storage.storagemanager.implementations.sql.KeyValueSQL;
import com.ibm.storage.storagemanager.interfaces.KeyValue;
import com.ibm.storage.storagemanager.performancetester.PerformanceTester;
import com.ibm.storage.storagemanager.util.Constants;
import com.ibm.storage.storagemanager.util.Util;


/**
 * @author ArunIyengar
 * Tests to generate performance information.  allTests will run tests across all storage systems.  It
 * is a proper superset of cloudTest which is a proper superset of fileTest
 *
 */
public class PerformanceTests {

    // File with performance tester configuration data should be entered here
    private static final String CONFIG_FILE_PERFORMANCETESTER =  Util.configFile(Constants.PERFORMANCETEST);

    // File with cloudant URL, username, and password should be entered here
    private static final String CONFIG_FILE_CLOUDANT =  Util.configFile(Constants.CLOUDANT);

    // File with object storage URL, username, and password should be entered here
    private static final String CONFIG_FILE_OBJECTSTORAGE =  Util.configFile(Constants.OBJECTSTORAGE);

    // File with SQL database name, base URL, username, and password should be entered here
    private static final String CONFIG_FILE_SQL =  Util.configFile(Constants.SQLID);
  

    KeyValueCloudant<String, byte[]> datastoreCloudant;
    KeyValueFile<String, byte[]> datastoreFile;
    KeyValueObjectStorage<String, byte[]> datastoreObjectstorage;
    KeyValueRedis<String, byte[]> datastoreRedis;
    KeyValueSQL<String, byte[]> datastoreSQL;    
    ArrayList<KeyValue<String, byte[]>> dataStores = new ArrayList<KeyValue<String, byte[]>>();
    
    /*
     * Test KeyValueFile
     */
    @Test
    public void fileTest() throws Exception {
        datastoreFile = new KeyValueFile<String, byte[]>("db1", true);
        dataStores.add(datastoreFile);        
        PerformanceTester.runAllTests(CONFIG_FILE_PERFORMANCETESTER, dataStores);
    }

    /*
     * Test KeyValueFile and the cloud stores, KeyValueCloudant and KeyValueObjectStorage.  Tests
     * are run in alphabetic order by data store name.
     */
    @Test
    public void cloudTest() throws Exception {
        datastoreCloudant = new KeyValueCloudant<String, byte[]>("db1", CONFIG_FILE_CLOUDANT, true);
        dataStores.add(datastoreCloudant);
        datastoreFile = new KeyValueFile<String, byte[]>("db1", true);
        dataStores.add(datastoreFile);
        datastoreObjectstorage = new KeyValueObjectStorage<String, byte[]>("db1",
                CONFIG_FILE_OBJECTSTORAGE, true);
        dataStores.add(datastoreObjectstorage);
        PerformanceTester.runAllTests(CONFIG_FILE_PERFORMANCETESTER, dataStores);
    }

    /*
     * Test KeyValueCloudant, KeyValueFile, KeyValueObjectStorage, KeyValueRedis, KeyValueSQL
     * remote cache results for KeyValueRedis will not be meaningful; all other results should be meaningful
     */
    @Test
    public void allTests() throws Exception {
        datastoreCloudant = new KeyValueCloudant<String, byte[]>("db1", CONFIG_FILE_CLOUDANT, true);
        dataStores.add(datastoreCloudant);
        datastoreFile = new KeyValueFile<String, byte[]>("db1", true);
        dataStores.add(datastoreFile);
        datastoreObjectstorage = new KeyValueObjectStorage<String, byte[]>("db1",
                CONFIG_FILE_OBJECTSTORAGE, true);
        dataStores.add(datastoreObjectstorage);
        datastoreRedis = new KeyValueRedis<String, byte[]>("localhost", 6379, 60);
        dataStores.add(datastoreRedis);
        datastoreSQL = new KeyValueSQL<String, byte[]>("db1", CONFIG_FILE_SQL, true);
        dataStores.add(datastoreSQL);
        PerformanceTester.runAllTests(CONFIG_FILE_PERFORMANCETESTER, dataStores);
    }
    
}
