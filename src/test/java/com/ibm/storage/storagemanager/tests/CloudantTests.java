package com.ibm.storage.storagemanager.tests;

import java.util.HashMap;

import org.junit.Test;

import com.ibm.storage.storagemanager.implementations.cloudant.KeyValueCloudant;
import com.ibm.storage.storagemanager.interfaces.KeyValue;
import com.ibm.storage.storagemanager.util.Constants;
import com.ibm.storage.storagemanager.util.Util;

public class CloudantTests {
    
    // File with cloudant URL, username, and password should be entered here
    private static final String CONFIG_FILE = Util.configFile(Constants.CLOUDANT);
    
    KeyValue<String, Integer> datastore = new KeyValueCloudant<String, Integer>("db1",
            CONFIG_FILE, true);
    
    KeyValue<String, HashMap<String, Integer>> datastore2 = 
            new KeyValueCloudant<String, HashMap<String, Integer>>("db1", CONFIG_FILE, true);

    @Test
    public void testPut() {
        StorageTests.testPut(datastore);
    }

    @Test
    public void testClear() {
        StorageTests.testClear(datastore);
    }

    @Test
    public void testDelete() {
        StorageTests.testDelete(datastore);
    }

    @Test
    public void testPutAll() {
        StorageTests.testPutAll(datastore);
    }

    @Test
    public void testGetAll() {
        StorageTests.testGetAll(datastore);
    }

    @Test
    public void testUpdate() {
        StorageTests.testUpdate(datastore);
    }
 
    @Test
    public void testHashMap() {
        StorageTests.testHashMap(datastore2);
    }
    
}
