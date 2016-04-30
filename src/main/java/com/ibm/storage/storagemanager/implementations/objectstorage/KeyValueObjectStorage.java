package com.ibm.storage.storagemanager.implementations.objectstorage;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

import com.ibm.storage.storagemanager.util.Constants;
import com.ibm.storage.storagemanager.util.Serializer;
import com.ibm.storage.storagemanager.util.Util;



/**
 * @author ArunIyengar
 * 
 */public class KeyValueObjectStorage<K,V>  implements com.ibm.storage.storagemanager.interfaces.KeyValue<K, V> {

     private Account account;
     private Container container;

     /**
      * Constructor. Establishes a session with a Key-value store, reading in credentials from a file.
      * 
      * @param dbName
      *            identifies the database
      * @param inputFile
      *            Name of file storing Cloudant URL, user id, and password
      * @param clearAll
      *            true if all previous database entries should be deleted
      */
     public KeyValueObjectStorage(String dbName, String inputFile, boolean clearAll) {
         File file = new File(inputFile);
         Scanner input = null;
         try {
             
             input = new Scanner(file);
         } catch (FileNotFoundException e) {
             System.out.println("Error in Object Storage KeyValue consstructor: File " + inputFile + " not found.  Exiting.");
             System.exit(1);
         }
         String storeId = Util.getNextWord(input);
         String userId = Util.getNextWord(input);
         String password = Util.getNextWord(input);
         createAccount(dbName, storeId, userId, password, clearAll);
     }

     /**
      * Constructor. Establishes a session with a Key-value store.
      * 
      * @param dbName
      *            identifies the database
      * @param storeId
      *            Url for the data store
      * @param userId
      *            user ID
      * @param password
      *            password
      * @param clearAll
      *            true if all previous database entries should be deleted
      */
     public KeyValueObjectStorage(String dbName, String storeId, String userId, String password, boolean clearAll) {
         createAccount(dbName, storeId, userId, password, clearAll);
     }

     private void createAccount(String dbName, String storeId, String userId, String password, boolean clearAll) {
         account = new AccountFactory()
         .setAuthenticationMethod(AuthenticationMethod.BASIC)
         .setUsername(userId)
         .setPassword(password)
         .setAuthUrl(storeId)
         .createAccount();
         container = account.getContainer(dbName);
         if (clearAll) {
             clearAll(container);
         }
         createContainer(container);
     } 
     
     private static void createContainer(Container cont) {
         if (!cont.exists()) {
             cont.create();
             cont.makePublic();
         }
     }
     
     private static void deleteContainer(Container cont) {
         if (cont.exists()) {
             clearAll(cont);
             cont.delete();
         }
     }
     
     private static void clearAll(Container container) {
         if (container.exists()) {
             for (StoredObject object : container.list()) {
                 object.delete();
             }
         }
     }
     
     /**
      * Output all container (database) names corresponding to the account
      * 
      * */
     public void printContainerNames() {
         Collection<Container> containers = account.list();
         for (Container currentContainer : containers) {
             System.out.println(currentContainer.getName());
         }
         
     }
     
     /**
      * delete all entries from the storage service
      * 
      * @return status code
      * 
      * */
     @Override
     public ReturnStatus clear() { 
         clearAll(container);
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
         String keyString = (String) key;
         StoredObject object = container.getObject(keyString);
         if (object.exists()) {
             object.delete();
             return 1;
         }
         else {
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
         String keyString = (String) key;
         StoredObject object = container.getObject(keyString);
         if (object.exists()) {
             byte[] rawValue = object.downloadObject();;
             if (rawValue == null) {
                 return null;
             }
             return Serializer.deserializeFromByteArray(rawValue);        
         }
         return null;
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
      * Return underlying container for applications to explicitly use.
      * 
      * @return value underlying Container object representing database
      * 
      * */
     public Container getDatabase() {
         return container;
     }
     
     /**
      * Return a string idenfitying the type of storage service
      * 
      * @return string identifying the type of storage service
      * */
     @Override
     public String storeType() {
         return Constants.OBJECTSTORAGE;
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
         String keyString = (String) key;
         StoredObject object = container.getObject(keyString);
         byte[] array = Serializer.serializeToByteArray(value);
         object.uploadObject(array);
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
      * Return number of stored objects
      * 
      * @return number of stored objects
      * */
     @Override
     public long size() {
         if (container.exists()) {  // has to be called to work properly
             // Collection<?> containerList = container.list();
             // return (containerList.size());
             return container.getCount();
         }
         else {
             return 0;
         }
     }
     
     /**
      * Output contents of current database to a string.
      * 
      * @return string containing output
      * 
      * */
     @Override
     public String toString() {
         return getContainerObjects(container);
     }
     
     private static void printObjectInfo(StoredObject object) {
         System.out.println("Public URL: "+object.getPublicURL());
         System.out.println("Last modified:  "+object.getLastModified());
         System.out.println("ETag:           "+object.getEtag());
         System.out.println("Content type:   "+object.getContentType());
         System.out.println("Content length: "+object.getContentLength());
     }
 
     private static String getMetadata(boolean useExtraIndent, Map<?, ?> metadata) {
         String indentString = useExtraIndent ? "    " : "";

         String result = indentString;
         if (metadata.isEmpty()) {
             result += "(there is no metadata)\n";
         } else {
             result += "Metadata:\n";
             for (Object entry : metadata.entrySet()) {
                 result += indentString;
                 result += String.format("  %s: %s%n", ((Map.Entry<?, ?>)entry).getKey(),
                         ((Map.Entry<?, ?>)entry).getValue());
             }
         }
         return result;
     }

     private static void printMetadata(boolean useExtraIndent, Map<?, ?> metadata) {
         String indentString = useExtraIndent ? "    " : "";

         System.out.print(indentString);
         if (metadata.isEmpty()) {
             System.out.println("(there is no metadata)");
         } else {
             System.out.println("Metadata:");
             for (Object entry : metadata.entrySet()) {
                 System.out.print(indentString);
                 System.out.printf("  %s: %s%n", ((Map.Entry<?, ?>)entry).getKey(),
                         ((Map.Entry<?, ?>)entry).getValue());
             }
         }
     }
     
     private static String getContainerObjects(Container container) {
         container.exists();  // certain container operations won't work w/o calling this
         String result = ("Container size: " + container.getCount()) + "\n";
         if (container.getCount() > 0) {
             result += "Contents:\n";
             for (StoredObject object : container.list()) {
                 result += String.format("  %s%n", object.getName());
                 if (container.isPublic()) {
                     result += String.format("    Public URL: %s%n", object.getPublicURL());
                 }
                 result += String.format("    Type: %s%n    Size: %s%n    Last modified: %s%n    E-tag: %s%n", object.getContentType(), object.getContentLength(),
                                   object.getLastModified(), object.getEtag());
                 result += getMetadata(true, object.getMetadata());
             }
         }
         return result + "\n";
     }
     
     private static void listContainerObjects(Container container) {
         container.exists(); // certain container operations won't work w/o calling this
         System.out.println("Container size: " + container.getCount());
         if (container.getCount() > 0) {
             System.out.println("Contents:");
             for (StoredObject object : container.list()) {
                 System.out.printf("  %s%n", object.getName());
                 if (container.isPublic()) {
                     System.out.printf("    Public URL: %s%n", object.getPublicURL());
                 }
                 System.out.printf("    Type: %s%n    Size: %s%n    Last modified: %s%n    E-tag: %s%n", object.getContentType(), object.getContentLength(),
                                   object.getLastModified(), object.getEtag());
                 printMetadata(true, object.getMetadata());
             }
         }

     }

     private static void test1(KeyValueObjectStorage<String, Integer> os1) {
         String key1 = "key1";
         String key2 = "key2";
         int val1 = 7;
         int val2 = 90;
         byte[] ba1, ba2;
         ba1 = Serializer.serializeToByteArray(val1);
         os1.printContainerNames();
         Container cont1 = os1.container;
         StoredObject object, obj2;
         
         obj2 = cont1.getObject(key2);
         if (obj2.exists()) {
             ba2 = obj2.downloadObject();
             val2 = Serializer.deserializeFromByteArray(ba2);
             System.out.println("Fetched Value of " + key1 + " is: " + val2);
         }
         else {
             System.out.println("object does not exist");
         }     
         object = cont1.getObject(key2);
         object.uploadObject(ba1);
         obj2 = cont1.getObject(key2);
         ba2 = obj2.downloadObject();
         val2 = Serializer.deserializeFromByteArray(ba2);
         System.out.println("Fetched Value of " + key1 + " is: " + val2);
         listContainerObjects(os1.container); // following line should produce same output
         System.out.println(getContainerObjects(os1.container)); 
     }
     
     private static void test2(KeyValueObjectStorage<String, Integer> os1) {
         os1.printContainerNames();
         deleteContainer(os1.container);
         System.out.println("containers after deleting test1");
         os1.printContainerNames();
     }
     
     public static void main(String[] args) {
         String configFile = Util.configFile(Constants.OBJECTSTORAGE);
         String dbName = "test1";
         KeyValueObjectStorage<String, Integer> os1 = new KeyValueObjectStorage<String, Integer>(dbName, configFile, true);
         test1(os1);
         System.out.println(os1.storeType());
         // test2(os1);
     }
}
