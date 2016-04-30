package com.ibm.storage.storagemanager.implementations.guava;

import java.util.List;
import java.util.Map;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.ibm.storage.storagemanager.util.Constants;

import static com.ibm.storage.storagemanager.util.Constants.NUM_UNKNOWN;

/**
 * @author ArunIyengar
 * 
 */
public class KeyValueGuava<K,V> implements com.ibm.storage.storagemanager.interfaces.KeyValue<K, V> {

    private LoadingCache<K,V> cache;

    /**
     * Constructor
     * 
     * @param maxObjects
     *            maximum number of objects which can be stored before
     *            replacement starts
     * 
     * */
    public KeyValueGuava(long maxObjects) {
        cache = CacheBuilder.newBuilder().maximumSize(maxObjects)
                .build(new CacheLoader<K,V>() {
                    public V load(K key) throws Exception {
                        return null;
                    }
                });
    }
  
    /**
     * delete all entries from the storage service
     * 
     * @return status code
     * 
     * */
   @Override
   public ReturnStatus clear() {
        cache.invalidateAll();
        return ReturnStatus.SUCCESS;
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
        cache.invalidate(key);
        return NUM_UNKNOWN;
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
        cache.invalidateAll(keys);        
        return NUM_UNKNOWN;
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
        return cache.getIfPresent(key);
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
        return  cache.getAllPresent(keys);
    }

    /**
     * Return a string idenfitying the type of storage service
     * 
     * @return string identifying the type of storage service
     * */
    @Override
    public String storeType() {
        return Constants.GUAVA;
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
        cache.put(key, value);
        return ReturnStatus.SUCCESS;
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
        cache.putAll(map);
        return NUM_UNKNOWN;
    }
 
    /**
     * Return number of objects in cache
     * 
     * */
    @Override
    public long size() {
        return cache.size();
    }

    /**
     * Return contents of entire cache in a string
     * 
     * @return string containing output
     * 
     * */
    @Override
    public String toString() {
        Map<K,V> cacheMap = cache.asMap();
        String result = "\nContents of Entire Cache\n\n";
        for (Map.Entry<K,V> entry : cacheMap.entrySet()) {
            result = result + "Key: " + entry.getKey() + "\n";
            V cacheEntry = entry.getValue();
            if (cacheEntry == null) {
                result = result + "CacheEntry is null\n";
            }
            else {
                result = result + cacheEntry.toString();
            }
            result = result + "\n\n";
        }
        result = result + "Cache size is: " + size() + "\n";
        return result;
    }
    
}
