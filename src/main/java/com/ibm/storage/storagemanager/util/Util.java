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
    
    private final static String SEPARATOR = "#"; // to separate comments from input values 
                                                 // in input files

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

      /**
       * Print a message and throw an exception
       * 
       * @param message
       *            message to print
       * 
       * */
      public static void throwException(String message) throws Exception {
          System.out.println(message);
          throw new Exception();
      }

      /**
       * read next input string
       * 
       * @param scan
       *            Scanner corresponding to input
       * 
       * @return next input string
       * 
       * */
      public static String getNextInput(Scanner scan) {
          String word = scan.nextLine();
          String word2 = (word.split(SEPARATOR)[0]);
          return word2.trim();
      }    

      /**
       * Read in a positive integer, which may come from a configuration file
       * 
       * @param message
       *            output message if read fails
       * @param input
       *            scanner corresponding to input
       *            
       * @return positive integer read in
       * 
       * */
      public static int readPositiveInt (String message, Scanner input) throws Exception {
          int value = Integer.parseInt(getNextInput(input));
          if (value < 1) {
              throwException("Error.  Illegal configuration parameter: " + message + ": " + value);
          }
          return value;
      }

}
