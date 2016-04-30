package com.ibm.storage.storagemanager.util;

import com.ibm.storage.storagemanager.interfaces.KeyValue;
import com.ibm.storage.storagemanager.interfaces.KeyValue.ReturnStatus;

import java.util.HashMap;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * @author ArunIyengar 
 * 
 */
/*
 * Utility methods called by several other classes
 */

public class Util {
    /**
     * Perform an unchecked cast while suppressing warnings
     * 
     * @param obj
     *            object to be cast
     * @param <T>
     *            type of cast object
     * @return cast object
     * 
     * */
    @SuppressWarnings({"unchecked"})
    public static <T> T uncheckedCast(Object obj) {
        return (T) obj;
    }

    public static String getNextWord(Scanner scan) {
        String word = scan.nextLine();
        return word.trim();
    }

    /**
     * Output information about an exception
     * 
     * @param e
     *            The exception
     * @param message
     *            Message to output
     * 
     * */
    public static void describeException(Exception e, String message) {
        System.out.println(message);
        System.out.println(e.getMessage());
        e.printStackTrace();
    }
    
    /**
     * delete a list of key-value pairs from a store
     * 
     * @param store
     *            data structure representing the store
     * @param keys
     *            iterable data structure containing the keys to delete
     * 
     * @return # of objects deleted
     * 
     * */
    public static <K, V> int deleteAll(KeyValue<K, V> store, List<K> keys) {
        int numDeleted = 0;
        for (K key : keys) {
            numDeleted += store.delete(key);
        }
        return numDeleted;
    }

    /**
     * look up one or more values in a store.
     * 
     * @param store
     *            data structure representing the store
     * @param keys
     *            iterable data structure containing the keys to look up
     *            
     * @return map containing key-value pairs corresponding to data in
     *         the store
     * 
     * */
    public static <K, V> Map<K, V> getAll(KeyValue<K, V> store, List<K> keys) {
        Map<K, V> hashMap = new HashMap<K, V>();
        for (K key : keys) {
            V value = store.get(key);
            if (value != null) {
                hashMap.put(key, value);
            }
        }
        return hashMap;
    }

    /**
     * store one or more key-value pairs
     * 
     * @param store
     *            data structure representing the store
     * @param map
     *            map containing key-value pairs to store
     * 
     * @return # of objects stored
     * 
     * */
    public static <K, V> int putAll(KeyValue<K, V> store, Map<K, V> map) {
        int numStored = 0;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (store.put(entry.getKey(), entry.getValue()) == ReturnStatus.SUCCESS) {
                numStored++;
            }
        }
        return numStored;
    }

    /**
     * Return the current time
     * 
     * @return Milliseconds since January 1, 1970
     * 
     * */
      public static long getTime() {
          return System.currentTimeMillis();
        // return (new Date()).getTime();
        //java.util.Date.getTime() method returns how many milliseconds have passed since 
        //January 1, 1970, 00:00:00 GMT
    }

      /**
       * return path name for configuration file
       * 
       * @param storeId
       *            string identifying the store
       * 
       * @return path name of configuration file
       * 
       * */
      public static String configFile(String storeId) {
          return Constants.CONFIG_DIRECTORY + File.separator + storeId + Constants.CONFIG_FILE_SUFFIX;
      }

      
}
