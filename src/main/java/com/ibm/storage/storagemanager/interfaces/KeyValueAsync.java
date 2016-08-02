/**
 * 
 */
package com.ibm.storage.storagemanager.interfaces;

import java.util.concurrent.Future;
import java.util.List;
import java.util.Map;


/**
 * @author ArunIyengar
 *
 */
public interface KeyValueAsync<K,V> extends KeyValue<K,V> {
 
    /**
     * delete all entries from the storage service asynchronously
     * 
     * @return Future for status code
     * 
     * */
    public Future<ReturnStatus> clearAsync();
    
    /**
     * delete a key-value pair asynchronously
     * 
     * @param key
     *            key corresponding to value
     * 
     * @return Future for # of objects deleted, NUM_UNKNOWN if unknown
     * 
     * */
    public Future<Integer> deleteAsync(K key);

    /**
     * delete one or more key-value pairs
     * 
     * @param keys
     *            iterable data structure containing the keys to delete
     * 
     * @return Future for # of objects deleted, NUM_UNKNOWN if unknown
     * 
     * */
    public Future<Integer> deleteAllAsync(List<K> keys);

    /**
     * look up a value asynchronously
     * 
     * @param key
     *            key corresponding to value
     * @return Future for value corresponding to key, future value null if key is not present
     * 
     * */
    public Future<V> getAsync(K key);
    
    /**
     * look up one or more values asynchronously.
     * 
     * @param keys
     *            iterable data structure containing the keys to look up
     * @return Future for map containing key-value pairs corresponding to data
     * 
     * */
    public Future<Map<K, V>> getAllAsync(List<K> keys);

    /**
     * Return size of thread pool supporting asynchronous interface
     * 
     * @return integer containing thread pool size
     * 
     * */
    public int getThreadPoolSize();

    /**
     * store a key-value pair asynchronously
     * 
     * @param key
     *            key associated with value
     * @param value
     *            value associated with key
     * 
     * @return Future for status code
     * 
     * */
    public Future<ReturnStatus> putAsync(K key, V value);

    /**
     * store one or more key-value pairs asynchronously
     * 
     * @param map
     *            map containing key-value pairs to store
     * 
     * @return Future for # of objects stored, NUM_UNKNOWN if unknown
     * 
     * */
    public Future<Integer> putAllAsync(Map<K, V> map);

    /**
     * Return number of stored objects asynchronously
     * 
     * @return Future for number of stored objects
     * */
    public Future<Long> sizeAsync();
    
    /**
     * Output contents of current database to a string asynchronously
     * 
     * @return Future for string containing output
     * 
     * */
    public Future<String> toStringAsync();

}
