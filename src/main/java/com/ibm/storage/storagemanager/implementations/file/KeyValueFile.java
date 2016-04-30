package com.ibm.storage.storagemanager.implementations.file;


import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.ibm.storage.storagemanager.util.Constants;
import com.ibm.storage.storagemanager.util.Serializer;
import com.ibm.storage.storagemanager.util.Util;


/**
 * @author ArunIyengar
 * 
 */
public class KeyValueFile<K,V> implements com.ibm.storage.storagemanager.interfaces.KeyValue<K, V> {
    

    private final static String ROOT_DIRECTORY = "database";
    
    private String directory;
    private File fileHdl;
    
    /**
     * Constructor. Establishes a session with a Key-value store.
     * 
     * @param dirName
     *            identifies the directory for storing all data
     * @param clearAll
     *            true if all previous directory entries should be deleted
     */
    public KeyValueFile(String dirName, boolean clearAll) {
        directory = ROOT_DIRECTORY + File.separator + dirName;
        fileHdl = new File(directory);
        if (clearAll) {
            deleteDirectory();
        }
        createDirectory();
    }
  
    /**
     * delete all entries from the storage service
     * 
     * @return status code
     * 
     * */
   @Override
   public ReturnStatus clear() {
       deleteDirectory();
       createDirectory();
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
        Path path = getPath(key);
        try {
            boolean deleted = Files.deleteIfExists(path);
            if (deleted) {
                return 1;
            }
            else {
                return 0;
            }
        } catch (Exception e) {
            return 0;
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
    public int deleteAll(List<K> keys) {
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
        byte[] rawValue = null;
        try {
            rawValue =  fileToByteArray(key);
        } catch (Exception e) {
            return null;
        }
        return Serializer.deserializeFromByteArray(rawValue);        
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
        return Util.getAll(this, keys);
    }

    /**
     * Return a string idenfitying the type of storage service
     * 
     * @return string identifying the type of storage service
     * */
    @Override
    public String storeType() {
        return Constants.FILE;
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
        try {
            byteArrayToFile(Serializer.serializeToByteArray(value), key);
        } catch (Exception e) {
            return ReturnStatus.FAILURE;
        }
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
        return Util.putAll(this, map);
    }
 
    /**
     * Return number of objects in cache
     * 
     * */
    @Override
    public long size() {
        return fileHdl.list().length;
    }

    /**
     * Return contents of entire cache in a string
     * 
     * @return string containing output
     * 
     * */
    @Override
    public String toString() {
        String returnVal = "Contents of Directory\n";
        File[] filesList = fileHdl.listFiles();
        for (File file : filesList) {
            if (file.isFile()) {
                returnVal += file.getName() + "\n";
            }
        }
        return returnVal;
    }
    
    private void createDirectory() {
        fileHdl.mkdirs();        
    }
    
    private void deleteDirectory() {
        try {
            FileUtils.deleteDirectory(fileHdl);
        }
        catch (Exception e) {
            System.out.println("Directory " + directory + " could not be deleted");
        }
    }
    
    
    // Store byte array in a file
    private void byteArrayToFile(byte[] data, K key) throws Exception {
        try (FileOutputStream fos = new FileOutputStream(getFileName(key), false)) {
            fos.write(data);
        }
    }

    // Read byte array from a file
    private byte[] fileToByteArray(K key) throws Exception {
        Path path = Paths.get(getFileName(key));
        return Files.readAllBytes(path);
    }

    private String getFileName(K key) {
        return directory + File.separator + (String) key;
    }
    
    // Get a Path handle for a particular key
    private Path getPath(K key) {
        return  Paths.get(getFileName(key));
    }
    
    public static void main(String[] args) throws Exception {
        String dir = ROOT_DIRECTORY + File.separator + "dir17"; 
        String dbName = "db1";
        
        System.out.println("Root directory is: " + System.getProperty("user.dir"));
        KeyValueFile<String, Integer> os1 = new KeyValueFile<String, Integer>(dbName, false);
        System.out.println("Database size: " + os1.size());
        os1.clear();
        System.out.println(os1.storeType());
        System.out.println("KeyValue.main finished executing");  // print root directory
    }
}
