package com.ibm.storage.storagemanager.tests;

import java.util.Arrays;

import org.junit.Test;

import com.ibm.storage.storagemanager.implementations.cloudant.KeyValueCloudant;
import com.ibm.storage.storagemanager.implementations.monitor.MonitoredKeyValue;
import com.ibm.storage.storagemanager.implementations.monitor.RequestStats;
import com.ibm.storage.storagemanager.implementations.monitor.StorageStats;
import com.ibm.storage.storagemanager.util.Constants;
import com.ibm.storage.storagemanager.util.Util;
import com.ibm.storage.storagemanager.util.Constants.RequestType;

public class MonitoredTests {

    // File with cloudant URL, username, and password should be entered here
    private static final String CONFIG_FILE =  Util.configFile(Constants.CLOUDANT);
    
    KeyValueCloudant<String, Integer> datastore2 = new KeyValueCloudant<String, Integer>("db1",
            CONFIG_FILE, true);

    MonitoredKeyValue<String, Integer> datastore = new MonitoredKeyValue<String, Integer>(datastore2, 10);

    @Test
    public void testUpdate() {      
        String key1 = "key1";

        System.out.println("testUpdate: start");
        datastore.clear();
        datastore.put(key1, 42);
        Integer val1 = datastore.get(key1);
        System.out.println(datastore.toString());
        datastore.put(key1, 43);
        val1 = datastore.get(key1);
        System.out.println(datastore.toString());
        datastore.put(key1, 44);
        val1 = datastore.get(key1);
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            
        }
        System.out.println(datastore.toString());
        StorageStats stats = datastore.getStorageStats();
        System.out.println(stats.allStats());
        System.out.println("Total number of requests: " + stats.getNumRequests());
        System.out.println("Total time spent on storage: " + stats.getTotalRequestTime());
        System.out.println("Start time: " + stats.getStartTime());
        System.out.println("End time: " + stats.getEndTime());
        stats.setEndTimeNow();
        System.out.println("New End time: " + stats.getEndTime());
        System.out.println("Store type: " + stats.getStoreType());
        RequestStats stats2 = stats.getRequestData(RequestType.PUT);
        System.out.println("Statistics for request type: " + stats2.getRequestType());
        System.out.println("Number of requests: " + stats2.getNumRequests());
        System.out.println("Time taken by requests: " + stats2.getTotalRequestTime());
        System.out.println("Recent request times: " + Arrays.toString(stats2.getRecentRequestTimes()));
        System.out.println("testUpdate: end\n");
        try {
            Thread.sleep(300);
        } catch (Exception e) {
        }
        stats.setStartTimeNow();
        System.out.println("new start time: " + stats.getStartTime());
        datastore.clearStorageStats();
        stats = datastore.getStorageStats();
        System.out.println("Stats after being cleared");
        System.out.println(stats.allStats());
    }

    
}
