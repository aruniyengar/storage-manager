package com.ibm.storage.storagemanager.tests;

import java.util.HashMap;

import org.junit.Test;

import com.ibm.storage.storagemanager.implementations.async.AsyncKeyValue;
import com.ibm.storage.storagemanager.implementations.guava.KeyValueGuava;
import com.ibm.storage.storagemanager.interfaces.KeyValue;
import com.ibm.storage.storagemanager.interfaces.KeyValueAsync;


public class GuavaTestsAsync {

    public GuavaTestsAsync() throws Exception {
        System.out.println("Thread pool size is " + datastoreAsync.getThreadPoolSize());

    }
    
    int numObjects = 2000;
    KeyValue<String, Integer> datastore = new KeyValueGuava<String, Integer>(numObjects);
    KeyValueAsync<String, Integer> datastoreAsync = new AsyncKeyValue<String, Integer>(datastore);
    
    KeyValue<String, HashMap<String, Integer>> datastore2 = new KeyValueGuava<String, HashMap<String, Integer>>(numObjects);
    KeyValueAsync<String, HashMap<String, Integer>> datastoreAsync2 = 
            new AsyncKeyValue<String, HashMap<String,Integer>>(datastore2);
   
    @Test
    public void testPut() throws Exception {
        StorageTestsAsync.testPut(datastoreAsync);
    }

    @Test
    public void testClear() throws Exception {
        StorageTestsAsync.testClear(datastoreAsync);
    }

    @Test
    public void testDelete() throws Exception {
        StorageTestsAsync.testDelete(datastoreAsync);
    }

    @Test
    public void testPutAll() throws Exception {
        StorageTestsAsync.testPutAll(datastoreAsync);
    }

    @Test
    public void testGetAll() throws Exception {
        StorageTestsAsync.testGetAll(datastoreAsync);
    }

    @Test
    public void testUpdate() throws Exception {
        StorageTestsAsync.testUpdate(datastoreAsync);
    }
 
    @Test
    public void testHashMap() {
        StorageTestsAsync.testHashMap(datastoreAsync2);
    }

    
}
