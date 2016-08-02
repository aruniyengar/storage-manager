/**
 * 
 */
package com.ibm.storage.storagemanager.implementations.async;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.ibm.storage.storagemanager.interfaces.KeyValue;
import com.ibm.storage.storagemanager.interfaces.KeyValueAsync;
import com.ibm.storage.storagemanager.interfaces.KeyValue.ReturnStatus;
import com.ibm.storage.storagemanager.util.Constants;
import com.ibm.storage.storagemanager.util.Constants.RequestType;
import com.ibm.storage.storagemanager.util.Util;
import com.ibm.storage.storagemanager.implementations.guava.KeyValueGuava;


/**
 * @author ArunIyengar
 * Implementation of KeyValueAsync interface for asynchronous operations using
 * ListenableFuture. This allows users to implement callback methods after'
 * operations have completed, unlike normal futures 
 */
public class AsyncKeyValue<K,V> implements KeyValueAsync<K,V> {
    
    private final static String CONFIG_FILE = "async";
    // Configuration file name.  Configuration file contains size of thread pool

    
    private ListeningExecutorService service;
    private KeyValue<K,V> store;
    private int threadPoolSize;
    
    
    /**
     * Constructor.
     * 
     * @param kvStore
     *            object corresponding to store to be monitored
     *            
     */
    public AsyncKeyValue(KeyValue<K,V> kvStore) throws Exception {
        store = kvStore;
        String configFile = Util.configFile(CONFIG_FILE);
        File file = new File(configFile);
        try (Scanner input = new Scanner(file)) {
            threadPoolSize = Util.readPositiveInt("thread pool size", input);           
        }
        service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadPoolSize));
    }

    /**
     * delete all entries from the storage service asynchronously
     * 
     * @return ListenableFuture for status code
     * 
     * */
    @Override
    public ListenableFuture<ReturnStatus> clearAsync() {
        return service.submit(new Callable<ReturnStatus>() {     
            @Override
            public ReturnStatus call() {
                return store.clear();
            }
        });
    }   
    
    /**
     * delete a key-value pair asynchronously
     * 
     * @param key
     *            key corresponding to value
     * 
     * @return ListenableFuture for # of objects deleted, NUM_UNKNOWN if unknown
     * 
     * */
    @Override
    public ListenableFuture<Integer> deleteAsync(final K key) {
        return service.submit(new Callable<Integer>() {     
            @Override
            public Integer call() {
                return store.delete(key);
            }
        });
    }

    /**
     * delete one or more key-value pairs
     * 
     * @param keys
     *            iterable data structure containing the keys to delete
     * 
     * @return ListenableFuture for # of objects deleted, NUM_UNKNOWN if unknown
     * 
     * */
    @Override
    public ListenableFuture<Integer> deleteAllAsync(final List<K> keys) {
        return service.submit(new Callable<Integer>() {     
            @Override
            public Integer call() {
                return store.deleteAll(keys);
            }
        });
    }

    /**
     * look up a value asynchronously
     * 
     * @param key
     *            key corresponding to value
     * @return ListenableFuture for value corresponding to key, future value null if key is not present
     * 
     * */
    @Override
    public ListenableFuture<V> getAsync(final K key) {
        return service.submit(new Callable<V>() {     
            @Override
            public V call() {
                return store.get(key);
            }
        });
    }

    /**
     * look up one or more values asynchronously.
     * 
     * @param keys
     *            iterable data structure containing the keys to look up
     * @return ListenableFuture for map containing key-value pairs corresponding to data
     * 
     * */
    @Override
    public ListenableFuture<Map<K, V>> getAllAsync(final List<K> keys) {
        return service.submit(new Callable<Map<K,V>>() {     
            @Override
            public Map<K,V> call() {
                return store.getAll(keys);
            }
        });
    }

    /**
     * Return size of thread pool supporting asynchronous interface
     * 
     * @return integer containing thread pool size
     * 
     * */
    @Override
    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    /**
     * store a key-value pair asynchronously
     * 
     * @param key
     *            key associated with value
     * @param value
     *            value associated with key
     * 
     * @return ListenableFuture for status code
     * 
     * */
    @Override
    public ListenableFuture<ReturnStatus> putAsync(final K key, final V value) {
        return service.submit(new Callable<ReturnStatus>() {
            @Override
            public ReturnStatus call() {
                return store.put(key, value);
            }
        });
    }

    /**
     * store one or more key-value pairs asynchronously
     * 
     * @param map
     *            map containing key-value pairs to store
     * 
     * @return Future for # of objects stored, NUM_UNKNOWN if unknown
     * 
     * */
    public Future<Integer> putAllAsync(final Map<K, V> map) {
        return service.submit(new Callable<Integer>() {
            @Override
            public Integer call() {
                return store.putAll(map);
            }
        });
    }

    /**
     * Return number of stored objects asynchronously
     * 
     * @return Future for number of stored objects
     * */
    public Future<Long> sizeAsync() {
        return service.submit(new Callable<Long>() {
            @Override
            public Long call() {
                return store.size();
            }
        });
    }
    
    /**
     * Output contents of current database to a string asynchronously
     * 
     * @return Future for string containing output
     * 
     * */
    public Future<String> toStringAsync() {
        return service.submit(new Callable<String>() {
            @Override
            public String call() {
                return store.toString();
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
        return store.clear();
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
        return store.delete(key);
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
        return store.deleteAll(keys);
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
        return store.get(key);
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
        return store.getAll(keys);
    }

    /**
     * Return a string idenfitying the type of storage service
     * 
     * @return string identifying the type of storage service
     * */
    @Override
   public String storeType() {
        return store.storeType();
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
        return store.put(key, value);
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
        return store.putAll(map);
    }

    /**
     * Return number of stored objects
     * 
     * @return number of stored objects
     * */
    @Override
    public long size()  {
        return store.size();
    }
    
    /**
     * Output contents of current database to a string.
     * 
     * @return string containing output
     * 
     * */
    @Override
    public String toString() {
        return store.toString();
    }

    public static void testUpdate(AsyncKeyValue<String, Integer> datastore) throws Exception {
        
        String key1 = "key1";

        
        System.out.println("testUpdate: start");
        datastore.clear();
        datastore.put(key1, 42);
        Integer val1 = datastore.get(key1);
        System.out.println("val1 is " + val1);
        System.out.println(datastore.toString());
        datastore.put(key1, 43);
        val1 = datastore.get(key1);
        System.out.println("val1 is " + val1);
        System.out.println(datastore.toString());
        datastore.put(key1, 44);
        val1 = datastore.get(key1);
        System.out.println("val1 is " + val1);
        System.out.println("Thread pool size is " + datastore.threadPoolSize);
        ListenableFuture<ReturnStatus> rs = datastore.putAsync(key1, 700);
        rs.get();
        ListenableFuture<Integer> valFuture = datastore.getAsync(key1);
        System.out.println("val1 obtained from future is " + valFuture.get());
    }
    
    
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        KeyValueGuava<String, Integer> datastore = new KeyValueGuava<String, Integer>(2000);
        AsyncKeyValue<String,Integer> asyncStore = new AsyncKeyValue<String, Integer>(datastore);
        testUpdate(asyncStore);
        System.out.println("main has finshed executing");
    }

}
