package com.ibm.storage.storagemanager.tests;

import java.util.HashMap;

import org.junit.Test;

import com.ibm.storage.storagemanager.implementations.guava.KeyValueGuava;

public class GuavaTests {

    int numObjects = 2000;
    KeyValueGuava<String, Integer> datastore = new KeyValueGuava<String, Integer>(numObjects);
    
    KeyValueGuava<String, HashMap<String, Integer>> datastore2 = new KeyValueGuava<String, HashMap<String, Integer>>(numObjects);
    
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
