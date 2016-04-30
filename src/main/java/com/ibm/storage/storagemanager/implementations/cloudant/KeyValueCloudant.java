/**
 * 
 */
package com.ibm.storage.storagemanager.implementations.cloudant;

import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.cloudant.client.api.model.Response;
import com.ibm.storage.storagemanager.util.Constants;
import com.ibm.storage.storagemanager.util.Serializer;
import com.ibm.storage.storagemanager.util.Util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;


/**
 * @author ArunIyengar
 *
 */
public class KeyValueCloudant<K,V> implements com.ibm.storage.storagemanager.interfaces.KeyValue<K, V> {
    private CloudantClient client;
    private Database database;
    private String databaseName;
    private boolean describeExceptions = true;
    private int putAttempts = 3;  // # of times to try put before giving up



    /**
     * Constructor. Establishes a session with a Key-value store.
     * 
     * @param storeId
     *            identifies the store, could be a URL such as for cloudant
     * @param dbName
     *            identifies the database
     * @param userId
     *            identifies the user
     * @param password
     *            password corresponding to the user
     * @param deletePreviousDb
     *            true if previous version of dbName should be deleted
     */
    public KeyValueCloudant(String storeId, String dbName, String userId, String password, 
            boolean deletePreviousDb) {
        initialize(storeId, dbName, userId, password, deletePreviousDb);
    }

    private void initialize(String storeId, String dbName, String userId, String password, 
            boolean deletePreviousDb) {
        client = new CloudantClient(storeId, userId, password);
        if (deletePreviousDb) {
            deleteDatabase(dbName);
        }
        getDatabase(dbName);        
    }
    
    /**
     * Constructor. Establishes a session with a Key-value store, reading in credentials from a file.
     * 
     * @param dbName
     *            identifies the database
     * @param inputFile
     *            Name of file storing Cloudant URL, user id, and password
     * @param deletePreviousDb
     *            true if previous version of dbName should be deleted
     */
    public KeyValueCloudant(String dbName, String inputFile, boolean deletePreviousDb) {
        File file = new File(inputFile);
        Scanner input = null;
        try {
            
            input = new Scanner(file);
        } catch (FileNotFoundException e) {
            System.out.println("Error in Cloudant KeyValue constructor: File " +
                    inputFile + " not found.  Exiting.");
            System.exit(1);
        }
        String storeId = Util.getNextWord(input);
        String userId = Util.getNextWord(input);
        String password = Util.getNextWord(input);
        initialize(storeId, dbName, userId, password, deletePreviousDb);
    }

    
    /**
     * Establishes a session with a specific database of the Key-value store.  This method
     * is used to replace the existing database associated with the key-value store.
     * 
     * @param id
     *            name of database
     */
    public void getDatabase(String id) {
        database = client.database(id, true);
        databaseName = id;
    }

    
    /**
     * Create a new database in the key-value store
     * 
     * @param id
     *            name of database
     */
    public void createDatabase(String id) {
        client.database(id, true);
    }

    /**
     * Delete a database in the key-value store
     * 
     * @param id
     *            name of database
     */
    public void deleteDatabase(String id) {
        try {
            client.deleteDB(id, "delete database");
        } catch (Exception e) {
        }
    }    
    
    /**
     * Pass boolean parameter to determine whether to output exceptions
     * 
     * @param showExceptions
     *            true to print exceptions, false otherwise
     */
    public void outputExceptions(boolean showExceptions) {
        describeExceptions = showExceptions;
    }

    /**
     * Set number of put attempts before giving up on a put request
     * 
     * @param attempts
     *            number of attempts (ignored if < 1
     */
    public void setPutAttempts(int attempts) {
        if (attempts >= 1)
            putAttempts = attempts;
    }

    // look up an object in database, return null if not found
    private DatabaseObject find(String id, Database db) {
        DatabaseObject object = null;
        try {
            object = db.find(DatabaseObject.class, id);
        } catch (Exception e) {
            return null;
        }
        return object;
    }
   
    /**
     * Insert a key-value pair into a database
     * 
     * @param key
     *            string identifying data
     * @param value
     *            byte array containing data (byte array of length 0 used to
     *            represent null)
     * @return true if insert succeeds, false otherwise
     */
    private boolean insert(String key, byte[] value) {
        DatabaseObject obj = new DatabaseObject(key, value);
        Response response = null;
        try {
            response = database.save(obj);
            if (response != null) {
                return true;
            }
        } catch (Exception e) {
        }
        try {
            obj = database.find(DatabaseObject.class, key);
        } catch (Exception e) {
            return false;
        }
        if (obj == null) { // might never happen
            // no object corresponding to key exists
            return false;
        }
        obj.setValue(value);
        try {
            response = database.update(obj);
            if (response != null) {
                return true;
            }
        } catch (Exception e) {
        }
        return false;   
    }
    
    /**
     * delete all entries from the storage service
     * 
     * @return status code
     * 
     * */
    @Override
    public ReturnStatus clear() {
        deleteDatabase(databaseName);  
        createDatabase(databaseName); 
        return ReturnStatus.SUCCESS;
    };

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
        String stringKey = (String) key;
        DatabaseObject obj = find(stringKey, database);
        if (obj == null)  {
            // no object corresponding to key exists
            return 0;
        }
        else {
            try {
                database.remove(obj);
            } catch (Exception e) {
                if (describeExceptions) {
                    System.out
                            .println("KeyValue.delete failed.  "
                                    + e.getMessage() + " " + e.getStackTrace());
                }
                return 0;
            }
            return 1;
        }
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
    public int deleteAll(List<K> keys){
        
        return Util.deleteAll(this, keys);
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
        String stringKey = (String) key;
        DatabaseObject obj = find(stringKey, database);
        if (obj == null) {
            // no object corresponding to key exists
            return null;
        }
        else {
            return Serializer.deserializeFromByteArray(obj.getValue());
        }
        
    };

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
        return Util.getAll(this, keys);
    };

    /**
     * Return underlying Database object for applications to explicitly use.
     * 
     * @return value underlying Database object representing database
     * 
     * */
    public Database getDatabase() {
        return database;
    }
 
    /**
     * Return a string idenfitying the type of storage service
     * 
     * @return string identifying the type of storage service
     * */
    @Override
    public String storeType() {
        return Constants.CLOUDANT;
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
        String stringKey = (String) key;
        byte[] byteArray = Serializer.serializeToByteArray(value);
        for (int i = 0; i < putAttempts; i ++) {
            boolean inserted = insert(stringKey, byteArray);
            if (inserted) {
                return ReturnStatus.SUCCESS;                
            }
        }
        return ReturnStatus.FAILURE;
    };

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
    };

    /**
     * Return number of stored objects
     * 
     * @return number of stored objects
     * */
    @Override
    public long size() {
        /*
        List<DatabaseObject> allObjects = database.view("_all_docs").includeDocs(true)
                .query(DatabaseObject.class);
        return allObjects.size();
        */
        return database.info().getDocCount();
    };
    
    /**
     * Output contents of current database to a string.
     * 
     * @return string containing output
     * 
     * */
    @Override
    public String toString() {
        List<DatabaseObject> allObjects = database.view("_all_docs").includeDocs(true)
                .query(DatabaseObject.class);
        String contents = "Contents of entire database: ";
        for (DatabaseObject item : allObjects) {
            contents = contents + item.toString();
        }
        return contents;
    };
    
    private static void addHashMap(KeyValueCloudant<String, HashMap<String, Integer>> kv, String key, int seed) {
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
        System.out.println("fetched hash map is: " + hm2);
 
    }

    private static void printInfo(KeyValueCloudant<String, HashMap<String, Integer>> kv) {
        System.out.println("database size: " + kv.size());
        System.out.println("contents of database: ");
        System.out.println(kv.toString());        
    }
    private static void test3(KeyValueCloudant<String, HashMap<String, Integer>> kv) {
        String key1 = "key1";
        String key2 = "key2";

        addHashMap(kv, key1, 1);
        HashMap<String, Integer> hm2 = kv.get(key2);
        System.out.println("fetched hash map (should be null) is: " + hm2);
        printInfo(kv);
        addHashMap(kv, key1, 100);
        printInfo(kv);
        kv.delete(key1);
        kv.delete(key1);
        printInfo(kv);
    }
    
    
    /**
     * @param args
     */
    public static void main(String[] args) throws  Exception {
        String dbName = "db1";
        String configFile = Util.configFile(Constants.CLOUDANT);
        KeyValueCloudant<String, HashMap<String, Integer>> kv = new KeyValueCloudant<String, HashMap<String, Integer>>(
                dbName, configFile, true);
        test3(kv);
        System.out.println(kv.storeType());
//        kv = new KeyValue<String, HashMap<String, Integer>>("db1", "C:\\temp\\junk.txt", true);
        System.out.println("KeyValue.main has finished executing. End of program");

    }

}
