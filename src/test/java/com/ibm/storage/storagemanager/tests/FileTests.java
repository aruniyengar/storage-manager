package com.ibm.storage.storagemanager.tests;

import java.util.HashMap;

import org.junit.Test;

import com.ibm.storage.storagemanager.implementations.file.KeyValueFile;
import com.ibm.storage.storagemanager.interfaces.KeyValue;


public class FileTests {
    
    
    KeyValue<String, Integer> datastore = new KeyValueFile<String, Integer>("db1", true);
    
    KeyValue<String, HashMap<String, Integer>> datastore2 = 
            new KeyValueFile<String, HashMap<String, Integer>>("db1", true);

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
