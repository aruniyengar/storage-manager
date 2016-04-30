package com.ibm.storage.storagemanager.interfaces;

import java.util.List;
import java.util.Map;

/**
 * @author ArunIyengar 
 * 
 */

/*
 * Key-value interface
 */
public interface KeyValue<K,V> {

    /**
     * Encodes return status for storage methods
     * 
     * */
    public enum ReturnStatus {
        SUCCESS,
        FAILURE
    }
    
    /**
     * delete all entries from the storage service
     * 
     * @return status code
     * 
     * */
    public ReturnStatus clear();

    /**
     * delete a key-value pair
     * 
     * @param key
     *            key corresponding to value
     * 
     * @return # of objects deleted, NUM_UNKNOWN if unknown
     * 
     * */
    public int delete(K key);

    /**
     * delete one or more key-value pairs
     * 
     * @param keys
     *            iterable data structure containing the keys to delete
     * 
     * @return # of objects deleted, NUM_UNKNOWN if unknown
     * 
     * */
    public int deleteAll(List<K> keys);

    /**
     * look up a value
     * 
     * @param key
     *            key corresponding to value
     * @return value corresponding to key, null if key is not present
     * 
     * */
    public V get(K key);

    /**
     * look up one or more values.
     * 
     * @param keys
     *            iterable data structure containing the keys to look up
     * @return map containing key-value pairs corresponding to data
     * 
     * */
    public Map<K, V> getAll(List<K> keys);

    /**
     * Return a string idenfitying the type of storage service
     * 
     * @return string identifying the type of storage service
     * */
    public String storeType();
    
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
    public ReturnStatus put(K key, V value);

    /**
     * store one or more key-value pairs
     * 
     * @param map
     *            map containing key-value pairs to store
     * 
     * @return # of objects stored, NUM_UNKNOWN if unknown
     * 
     * */
    public int putAll(Map<K, V> map);

    /**
     * Return number of stored objects
     * 
     * @return number of stored objects
     * */
    public long size();
    
    /**
     * Output contents of current database to a string.
     * 
     * @return string containing output
     * 
     * */
    public String toString();

}
