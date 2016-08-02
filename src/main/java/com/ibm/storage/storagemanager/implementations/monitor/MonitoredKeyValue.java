/**
 * 
 */
package com.ibm.storage.storagemanager.implementations.monitor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.ibm.storage.storagemanager.interfaces.KeyValueMonitored;
import com.ibm.storage.storagemanager.interfaces.KeyValue;
import com.ibm.storage.storagemanager.util.Constants.RequestType;
import com.ibm.storage.storagemanager.util.Constants;
import com.ibm.storage.storagemanager.util.Util;


import com.ibm.storage.storagemanager.implementations.cloudant.KeyValueCloudant;


/**
 * @author ArunIyengar
 *
 */
public class MonitoredKeyValue<K,V> implements KeyValueMonitored<K, V> {
    private int historySize;
    private StorageStats stats;
    private KeyValue<K,V> store;

    /**
     * 
     */
    /**
     * Constructor.
     * 
     * @param kvStore
     *            object corresponding to store to be monitored
     * @param historyLength
     *            # of most recent data points to keep around for each transaction type
     *            
     */
    public MonitoredKeyValue(KeyValue<K,V> kvStore, int historyLength) {
        store = kvStore;
        historySize = historyLength;
        stats = new StorageStats(historySize, store.storeType());
    }

    /**
     * Reset the StorageStats object to the initial state
     * 
     * */
    public void clearStorageStats() {
        stats = new StorageStats(historySize, store.storeType());
    };
    
    
   /**
     * get data structure returning storage stats
     * 
     * @return data structure containing storage stats
     * 
     * */
    public StorageStats getStorageStats() {
        return stats;
    };
    
    
    /**
     * input new StorageStats data structure with customized values
     * 
     * @param stats
     *            data structure with customized values
     * 
     * */
    public void setStorageStats(StorageStats newStats) {
        stats = newStats;
    };
    
    
    /**
     * delete all entries from the storage service
     * 
     * @return status code
     * 
     * */
    @Override
    public ReturnStatus clear() {
        long startTime = Util.getTime();
        ReturnStatus returnVal = store.clear();
        long endTime = Util.getTime();
        stats.recordRequest(endTime - startTime, RequestType.CLEAR);
        return returnVal;
    }
  
    /**
     * delete a key-value pair
     * 
     * @param key
     *            key corresponding to value
     * 
     * @return # of objects deleted, NUM_UNKNOWN if unknown
     * 
     * */
    @Override
    public int delete(K key) {
        long startTime = Util.getTime();
        int returnVal = store.delete(key);
        long endTime = Util.getTime();
        stats.recordRequest(endTime - startTime, RequestType.DELETE);
        return returnVal;      
    }

    /**
     * delete one or more key-value pairs
     * 
     * @param keys
     *            iterable data structure containing the keys to delete
     * 
     * @return # of objects deleted, NUM_UNKNOWN if unknown
     * 
     * */
    @Override
    public int deleteAll(List<K> keys) {
        long startTime = Util.getTime();
        int returnVal = store.deleteAll(keys);
        long endTime = Util.getTime();
        stats.recordRequest(endTime - startTime, RequestType.DELETEALL);
        return returnVal;      
    }

    /**
     * look up a value
     * 
     * @param key
     *            key corresponding to value
     * @return value corresponding to key, null if key is not present
     * 
     * */
    @Override
    public V get(K key) {
        long startTime = Util.getTime();
        V returnVal = store.get(key);
        long endTime = Util.getTime();
        stats.recordRequest(endTime - startTime, RequestType.GET);
        return returnVal;      
    }

    /**
     * look up one or more values.
     * 
     * @param keys
     *            iterable data structure containing the keys to look up
     * @return map containing key-value pairs corresponding to data
     * 
     * */
    @Override
    public Map<K, V> getAll(List<K> keys) {
        long startTime = Util.getTime();
        Map<K, V> returnVal = store.getAll(keys);
        long endTime = Util.getTime();
        stats.recordRequest(endTime - startTime, RequestType.GETALL);
        return returnVal;      
    }

    /**
     * Return a string idenfitying the type of storage service
     * 
     * @return string identifying the type of storage service
     * */
    @Override
   public String storeType() {
        long startTime = Util.getTime();
        String returnVal = store.storeType();
        long endTime = Util.getTime();
        stats.recordRequest(endTime - startTime, RequestType.STORETYPE);
        return returnVal;      
    }
    
    /**
     * store a key-value pair
     * 
     * @param key
     *            key associated with value
     * @param value
     *            value associated with key
     * 
     * @return status code
     * 
     * */
    @Override
    public ReturnStatus put(K key, V value) {
        long startTime = Util.getTime();
        ReturnStatus returnVal = store.put(key, value);
        long endTime = Util.getTime();
        stats.recordRequest(endTime - startTime, RequestType.PUT);
        return returnVal;      
    }

    /**
     * store one or more key-value pairs
     * 
     * @param map
     *            map containing key-value pairs to store
     * 
     * @return # of objects stored, NUM_UNKNOWN if unknown
     * 
     * */
    @Override
    public int putAll(Map<K, V> map) {
        long startTime = Util.getTime();
        int returnVal = store.putAll(map);
        long endTime = Util.getTime();
        stats.recordRequest(endTime - startTime, RequestType.PUTALL);
        return returnVal;      
    }

    /**
     * Return number of stored objects
     * 
     * @return number of stored objects
     * */
    @Override
    public long size()  {
        long startTime = Util.getTime();
        long returnVal = store.size();
        long endTime = Util.getTime();
        stats.recordRequest(endTime - startTime, RequestType.SIZE);
        return returnVal;      
    }
    
    /**
     * Output contents of current database to a string.
     * 
     * @return string containing output
     * 
     * */
    @Override
    public String toString() {
        long startTime = Util.getTime();
        String returnVal = store.toString();
        long endTime = Util.getTime();
        stats.recordRequest(endTime - startTime, RequestType.TOSTRING);
        return returnVal;      
    }


    public static void testUpdate(MonitoredKeyValue<String, Integer> datastore) {
        
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

    
    /**
     * @param args
     */
    public static void main(String[] args) {
        String configFile = Util.configFile(Constants.CLOUDANT);
        KeyValue<String, Integer> datastore = new KeyValueCloudant<String, Integer>("db1",
                configFile, true);
        MonitoredKeyValue<String, Integer> ds1 = new MonitoredKeyValue<String, Integer>(datastore, 10);
        testUpdate(ds1);
        System.out.println("Program has finished executing");

    }

}
