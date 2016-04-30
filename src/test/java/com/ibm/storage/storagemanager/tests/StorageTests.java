package com.ibm.storage.storagemanager.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.ibm.storage.storagemanager.interfaces.KeyValue;

import static com.ibm.storage.storagemanager.util.Constants.NUM_UNKNOWN;


public class StorageTests {

    private static String key1 = "key1";
    private static String key2 = "key2";
    private static String key3 = "key3";

    private static void printEntry(KeyValue<String, Integer> datastore, String key) {
        System.out.println("key  is: " + key);
        Integer val = datastore.get(key);
        if (val == null) {
            System.out.println("Key " + key + " not in store");
        }
        else {
            System.out.println("Value is: " + val.toString());
        }
    }

    
    public static void testPut(KeyValue<String,Integer> datastore) {
        System.out.println("testPut: start");
        datastore.clear();
        datastore.put(key1, 42);
        datastore.put(key2, 43);
        KeyValue.ReturnStatus status = datastore.put(key3, 44);
        assertEquals("put should return success", KeyValue.ReturnStatus.SUCCESS,
                status);
        printEntry(datastore, key1);
        printEntry(datastore, key2);
        assertEquals("Size should be 3", 3, datastore.size());
        System.out.println("store size: " + datastore.size());
        assertEquals("Fetched value should be 43",(Integer) datastore.get(key2), (Integer) 43);
        System.out.println("testPut: end\n");
    }

    public static void testClear(KeyValue<String, Integer> datastore) {
        System.out.println("testClear: start");
        KeyValue.ReturnStatus status = datastore.clear();
        assertEquals("clear should return success", KeyValue.ReturnStatus.SUCCESS,
                status);
        datastore.put(key1, 42);
        datastore.put(key2, 43);
        datastore.put(key3, 44);
        assertEquals("Fetched value should be 44",(Integer) datastore.get(key3), (Integer) 44);
        assertEquals("Size should be 3", 3, datastore.size());
        System.out.println("store size: " + datastore.size());
        System.out.println(datastore.toString());
        datastore.clear();
        assertEquals("Fetched value should be null", datastore.get(key3), null);
        assertEquals("Size should be 0", 0, datastore.size());
        System.out.println("store size: " + datastore.size());
        System.out.println(datastore.toString());
        System.out.println("testClear: end\n");
    }

    public static void testDelete(KeyValue<String, Integer> datastore) {
        System.out.println("testDelete: start");
        datastore.clear();
        datastore.put(key1, 42);
        datastore.put(key2, 43);
        datastore.put(key3, 44);
        assertEquals("Size should be 3", 3, datastore.size());
        System.out.println(datastore.toString());
        int numDeleted = datastore.delete(key2);
        assertTrue((numDeleted == 1) || (numDeleted == NUM_UNKNOWN));
        assertEquals("Size should be 2", 2, datastore.size());
        System.out.println(datastore.toString());
        datastore.put(key2, 50);
        datastore.put("key4", 59);
        datastore.put("key5", 80);

        ArrayList<String> list = new ArrayList<String>();
        list.add(key1);
        list.add(key2);
        numDeleted = datastore.deleteAll(list);
        assertTrue((numDeleted == 2) || (numDeleted == NUM_UNKNOWN));
        assertEquals("Size should be 3", 3, datastore.size());
        datastore.delete("adjkfjadfjdf");
        numDeleted = datastore.delete("adfkasdklfjil");
        assertTrue((numDeleted == 0) || (numDeleted == NUM_UNKNOWN));
        assertEquals("Size should be 3", 3, datastore.size());
        System.out.println(datastore.toString());
        System.out.println("testDelete: end\n");
    }
    
    private static <K, V> void printMap(Map<K, V> map) {
        System.out.println("printMap: outputting map contents ");
        for (Map.Entry<K, V> entry : map.entrySet()) {
            System.out.println("Key: " + entry.getKey() + " Value: "
                    + entry.getValue());
        }
    }

    public static void testPutAll(KeyValue<String, Integer> datastore) {
        System.out.println("testPutAll: start");
        datastore.clear();
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        map.put(key1, 42);
        map.put(key2, 43);
        map.put(key3, 44);
        datastore.clear();
        int numStored = datastore.putAll(map);
        assertTrue((numStored == 3) || (numStored == NUM_UNKNOWN));
        System.out.println(datastore.toString());
        assertEquals("Size should be 3", 3, datastore.size());
        System.out.println("testPutAll: end\n");
    }

    public static void testGetAll(KeyValue<String, Integer> datastore) {
        System.out.println("testGetAll: start");
        datastore.clear();
        datastore.put(key1, 42);
        datastore.put(key2, 43);
        datastore.put(key3, 44);
        ArrayList<String> list = new ArrayList<String>();
        list.add(key1);
        list.add(key2);
        list.add(key3);
        Map<String, Integer> map = datastore.getAll(list);
        printMap(map);
        assertEquals("Returned map size should be 3", 3, map.size());
        System.out.println("testGetAll: end\n");
    }

    public static void testUpdate(KeyValue<String, Integer> datastore) {
        System.out.println("testUpdate: start");
        datastore.clear();
        datastore.put(key1, 42);
        Integer val1 = datastore.get(key1);
        assertEquals("Val1 should be 42, actual value is " + val1, 42, val1.intValue());
        System.out.println(datastore.toString());
        datastore.put(key1, 43);
        val1 = datastore.get(key1);
        assertEquals("Val1 should be 43, actual value is " + val1, 43, val1.intValue());
        System.out.println(datastore.toString());
        datastore.put(key1, 44);
        val1 = datastore.get(key1);
        assertEquals("Val1 should be 44, actual value is " + val1, 44, val1.intValue());
        System.out.println(datastore.toString());
        System.out.println("testUpdate: end\n");
    }
 
    private static void addHashMap(KeyValue<String, HashMap<String, Integer>> kv, String key, int seed) {
        int val1 = seed + 100;
        int val2 = seed + 200;
        String hashKey1 = "key" + seed;
        String hashKey2 = "key" + (seed + 1);
 
        HashMap<String, Integer> hm = new HashMap<String, Integer>();
        hm.put(hashKey1, val1);
        hm.put(hashKey2, val2);
        System.out.println("Original hash map is: " + hm);
        System.out.println(kv.put(key, hm));
        HashMap<String, Integer> hm2 = kv.get(key);
        assertEquals("Stored and fetched hash maps should have same size", hm.size(), hm2.size());
        System.out.println("fetched hash map is: " + hm2);
 
    }

    private static void printInfo(KeyValue<String, HashMap<String, Integer>> kv) {
        System.out.println("database size: " + kv.size());
        System.out.println("contents of database: ");
        System.out.println(kv.toString());        
    }
    public static void testHashMap(KeyValue<String, HashMap<String, Integer>> kv) {
        String key1 = "key1";
        String key2 = "key2";

        kv.clear();
        addHashMap(kv, key1, 1);
        HashMap<String, Integer> hm2 = kv.get(key2);
        assertEquals("Fetched hash map should be null", hm2, null);
        System.out.println("fetched hash map (should be null) is: " + hm2);
        printInfo(kv);
        addHashMap(kv, key1, 100);
        assertEquals("Store should have size 1", kv.size(), 1);
        printInfo(kv);
        kv.delete(key1);
        assertEquals("Store should have size 0", kv.size(), 0);
        kv.delete(key1);
        printInfo(kv);
        System.out.println("Type of storage service is: " + kv.storeType());
    }

    
}
