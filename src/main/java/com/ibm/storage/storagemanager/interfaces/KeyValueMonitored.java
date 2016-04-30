/**
 * 
 */
package com.ibm.storage.storagemanager.interfaces;

import com.ibm.storage.storagemanager.implementations.monitor.StorageStats;

/**
 * @author ArunIyengar
 *
 */
public interface KeyValueMonitored<K,V> extends KeyValue<K,V> {

    /**
     * Reset the StorageStats object to the initial state
     * 
     * */
    public void clearStorageStats();
    
    
   /**
     * get data structure returning storage stats
     * 
     * @return data structure containing storage stats
     * 
     * */
    public StorageStats getStorageStats();
    
    
    /**
     * input new StorageStats data structure with customized values
     * 
     * @param stats
     *            data structure with customized values
     * 
     * */
    public void setStorageStats(StorageStats newStats);

}
