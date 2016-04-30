package com.ibm.storage.storagemanager.implementations.redis;

import com.ibm.storage.storagemanager.util.Constants;
import com.ibm.storage.storagemanager.util.Serializer;
import com.ibm.storage.storagemanager.util.Util;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;


/**
 * @author ArunIyengar
 * 
 */
public class KeyValueRedis<K,V>  implements com.ibm.storage.storagemanager.interfaces.KeyValue<K, V> {
    private Jedis cache;
    
    private final static String DEFAULT_CHAR_SET = "UTF8";
    private final static String RETURN_SUCCESS = "OK";


    /**
     * Constructor creating Jedis instance
     * 
     * @param host
     *            host where Redis is running
     * 
     * */
    public KeyValueRedis(String host) {
        cache = new Jedis(host);
    }

    /**
     * Constructor creating Jedis instance
     * 
     * @param host
     *            host where Redis is running
     * @param port
     *            port number
     * 
     * */
    public KeyValueRedis(String host, int port) {
        cache = new Jedis(host, port);
    }

    /**
     * Constructor creating Jedis instance
     * 
     * @param host
     *            host where Redis is running
     * @param port
     *            port number
     * @param timeout
     *            number of seconds before Jedis closes an idle connection
     * 
     * */
    public KeyValueRedis(String host, int port, int timeout) {
        cache = new Jedis(host, port, timeout);
    }

    /**
     * Constructor in which already-created Jedis instance is passed in to be used as underlying
     * cache.  This constructor is for situations in which application wants access to 
     * Jedis instance so that it can directly make Jedis method calls on the Jedis instance.
     * 
     * @param jedisCache
     *            Existing Jedis instance to be used as underlying cache
     * 
     * */
    public KeyValueRedis(Jedis jedisCache) {
        cache = jedisCache;
    }

    
    /**
     * delete all key-value pairs from the current database
     * 
     * @return status code
     * 
     * */
    @Override
    public ReturnStatus clear() {
        return getStatus(cache.flushDB());
    }

    /**
     * Close a Redis connection
     * 
     * */
    public void close() {
        cache.close();
    }

    /**
     * delete a key-value pair from the cache
     * 
     * @param key
     *            key corresponding to value
     * 
     * @return # of objects deleted, NUM_UNKNOWN if unknown
     * 
     * */
    @Override
    public int delete(K key) {
        return (cache.del(Serializer.serializeToByteArray(key))).intValue();
    }

    /**
     * delete one or more key-value pairs from the cache
     * 
     * @param keys
     *            iterable data structure containing the keys to delete
     * 
     * @return # of objects deleted, NUM_UNKNOWN if unknown
     * 
     * */
    @Override
    public int deleteAll(List<K> keys) {
        return Util.deleteAll(this, keys);
    }

    /**
     * delete all key-value pairs from all databases
     * 
     * @return status code reply
     * 
     * */
    public String flushAll() {
        return cache.flushAll();
    }

    /**
     * look up a value in the cache
     * 
     * @param key
     *            key corresponding to value
     * @return value corresponding to key, null if key is not in cache or if
     *         value is expired
     * 
     * */
    @Override
    public V get(K key) {
        byte[] rawValue = cache.get(Serializer.serializeToByteArray(key));
        if (rawValue == null) {
            return null;
        }
        return Serializer.deserializeFromByteArray(rawValue);        
    }

    /**
     * look up one or more values in the cache.
     * 
     * @param keys
     *            iterable data structure containing the keys to look up
     * @return map containing key-value pairs corresponding to data in
     *         the cache
     * 
     * */
    @Override
    public Map<K, V> getAll(List<K> keys) {
        return Util.getAll(this, keys);
    }
    
    /**
     * Return underlying Jedis object for applications to explicitly use.
     * 
     * @return value underlying Jedis object representing cache
     * 
     * */
    public Jedis getDatabase() {
        return cache;
    }
        
    /**
     * Return string representing a stored entry corresponding to a key (or indicate if the
     * key is not stored). 
     * 
     * @param key
     *            key corresponding to value
     * @return string containing output
     * 
     * */ 
    public String printStoredEntry(K key) {
        String result = "value for key: " + key + "\n";
        V cacheEntry = get(key);
        if (cacheEntry == null) {
            result+= "Key " + key + " not stored" + "\n";
        }
        else {
            result += cacheEntry.toString();
        }
        return result;
    }
    
    /**
     * cache a key-value pair
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
        byte[] array1 = Serializer.serializeToByteArray(key);
        byte[] array2 = Serializer.serializeToByteArray(value);
        return getStatus(cache.set(array1, array2));
    }

    /**
     * Return a string idenfitying the type of storage service
     * 
     * @return string identifying the type of storage service
     * */
    @Override
    public String storeType() {
        return Constants.REDIS;
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
        return Util.putAll(this, map);
    }
    
    /**
     * Select the DB with having the specified zero-based numeric index.
     * 
     * @param index
     *            database index
     * @return status code response
     * 
     * */
    public String select(int index) {
        return cache.select(index);
    }
    
    /**
     * Return number of objects in cache
     * 
     * */
    @Override
    public long size() {
        return cache.dbSize();
    }

    /**
     * Output contents of current database to a string.
     * 
     * @param charset
     *            Character set representing keys, if not default
     * @return string containing output
     * 
     * */
    public String toString(String charset) {
        String result = "\nContents of Entire Store\n\n";
        Set<byte[]> keys = cache.keys(Serializer.serializeString("*", Charset.forName(charset)));
        for (byte[] key : keys) {
            String keyString = Serializer.deserializeFromByteArray(key);
            result += "Key: " + keyString + "\n";
            byte[] rawValue = cache.get(key);
            if (rawValue == null) {
                result += "No value found in store for keyString " + keyString + "\n\n";
                continue;
            }
            V cacheEntry = Serializer.deserializeFromByteArray(rawValue);
            if (cacheEntry == null) {
                result += "Value is null for keyString " + keyString + "\n\n";
                continue;
            }
            result += cacheEntry.toString() + "\n\n";
        }
        result += "# of objects in store is: " + size() + "\n";
        return result;
    }
    
    /**
     * Output contents of current database to a string.
     * 
     * @return string containing output
     * 
     * */
    @Override
    public String toString() {
        return toString(DEFAULT_CHAR_SET);
    }
    
    private ReturnStatus getStatus(String status) {
        if (status.equals(RETURN_SUCCESS)) 
            return ReturnStatus.SUCCESS;
        else
            return ReturnStatus.FAILURE;
    }
}
