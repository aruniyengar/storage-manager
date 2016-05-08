# storage-manager
Gives Java applications enhanced options for storing data.  Multiple options are provided for storing data including file systems, SQL stores, NoSQL stores, and caches.  A common key-value interface is provided to make it easy to switch between different data stores.  Performance monitoring of different data stores is provided.  In addition, a workload generator is provided to make it easy to compare the performance of different data stores.  

Caching is provided to improve performance.  Encryption is provided to preserve data confidentiality.  Compression is provided to reduce data sizes.

An overview of this storage client library is available from:
* Arun Iyengar, [Infrastructure Components for Efficient Data Management](http://domino.watson.ibm.com/library/CyberDig.nsf/papers/71C046EA5608285085257F9500647961/$File/rc25599.pdf), IBM Research Report RC25599 (WAT1604-013), April 7, 2016.

## Getting Started

###Constructing Objects to Acccess Data Stores
A common key-value interface is defined in com.ibm.storage.storagemanager.interfaces.KeyValue<K, V>.  This interface is implemented by multiple data stores.  That way, an application program which uses this interface can easily switch between different data store implementations.  In order to use a file system implementation of the key-value interface, the following can be used:
~~~ java
import com.ibm.storage.storagemanager.implementations.file.KeyValueFile;
import com.ibm.storage.storagemanager.interfaces.KeyValue;
    KeyValue<String, Integer> datastore = new KeyValueFile<String, Integer>("db1", true);
~~~
In this example, the keys are strings, and the values are integers.  Each value will be stored in a file whose name is the key.  The files will be in the directory database/db1.  The last parameter, “true”, indicates that all previous files stored in this directory should be deleted.

In order to use an implementation of the key-value interface in which a cache is used which runs in the same process as the application, the following can be used:
~~~ java
import com.ibm.storage.storagemanager.implementations.guava.KeyValueGuava;
import com.ibm.storage.storagemanager.interfaces.KeyValue;
    KeyValue<String, Integer> datastore = new KeyValueGuava<String, Integer>(numObjects);
~~~
where numObjects is the maximum number of objects which can be stored in the cache.  

In order to use an implementation of the key-value interface in which a Redis cache is used, a Redis cache needs to be running in a separate process.  The following can then be used:
~~~ java
import com.ibm.storage.storagemanager.implementations.redis.KeyValueRedis;
import com.ibm.storage.storagemanager.interfaces.KeyValue;
    KeyValue<String, Integer> datastore = new KeyValueRedis<String, Integer>("localhost", 6379, 60);
~~~
In this example, the cache is running on the same node as the application program, 6379 is the port number, and 60 indicates that idle connections should be closed after 60 seconds.

In order to use an implementation of the key-value interface in which a MySQL database is used,
~~~ java
import com.ibm.storage.storagemanager.implementations.sql.KeyValueSQL;
import com.ibm.storage.storagemanager.interfaces.KeyValue;
    KeyValue<String, Integer> datastore = new KeyValueSQL<String, Integer>("db1", CONFIG_FILE, true);
~~~
In this example, “db1” is the name of the database for storing the key-value data, CONFIG_FILE is a configuration text file, and true indicates that all previous values stored in the database should be deleted.  A skeleton for the configuration file is in the gitub repository file config/sql.config.  The first line is the name of the sql database for storing the data.  The second line is the url for accessing the database.  The third line is the user id, and the fourth line is the password.
In order to pass parameters directly to the KeyValueSQL constructor and avoid using a configuration file, the following constructor can be invoked:
~~~ java
    /**
     * Constructor. Establishes a session with a Key-value store..
     * 
     * @param tableName
     *            identifies the table name for the key-value pairs
     * @param databaseName
     *            database name
     * @param url
     *            base URL for the data service
     * @param userName
     *            user name for authentication
     * @param passWord
     *            password
     */
    public KeyValueSQL(String tblName, String databaseName, String url, String userName, String passWord,
            boolean clearAll);
~~~
	
In order to use an implementation of the KeyValue interface in which Cloudant is used,
~~~ java
import com.ibm.storage.storagemanager.implementations.cloudant.KeyValueCloudant;
import com.ibm.storage.storagemanager.interfaces.KeyValue;
    KeyValue<String, Integer> datastore = new KeyValueCloudant<String, Integer>("db1", CONFIG_FILE, true);
~~~
In this example, “db1” is the name of the database for storing the key-value data, CONFIG_FILE is a configuration text file, and true indicates that all previous values stored in the database should be deleted.  A skeleton for the configuration file is in the gitub repository file config/cloudant.config.  The first line is the url for the Cloudant account.  The second line is the user id, and the third line is the password.
In order to pass parameters directly to the KeyValueCloudant constructor and avoid using a configuration file, the following constructor can be invoked:
~~~ java
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
            boolean deletePreviousDb)
~~~

In order to use an implementation of the KeyValue interface in which OpenStack Object Storage is used,
~~~ java
import com.ibm.storage.storagemanager.implementations.objectstorage.KeyValueObjectStorage;
import com.ibm.storage.storagemanager.interfaces.KeyValue;
    KeyValue<String, Integer> datastore = new KeyValueObjectStorage<String, Integer>("db1", CONFIG_FILE, true);
~~~

In this example, “db1” is the name of the database (container) for storing the key-value data, CONFIG_FILE is a configuration text file, and true indicates that all previous values stored in the database should be deleted.  A skeleton for the configuration file is in the gitub repository file config/objectstorage.config.  The first line is the url for accessing Object Storage.  The second line is the user id, and the third line is the password.
In order to pass parameters directly to the KeyValueObjectStorage constructor and avoid using a configuration file, the following constructor can be invoked:
~~~ java
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
     public KeyValueObjectStorage(String dbName, String storeId, String userId, String password, boolean clearAll)
~~~

The same KeyValue methods are used for each implementation of datastore.  This makes it easy to substitute different implementations of data stores within an application program.  We next show examples of using datastore.  Note that the same method calls could be used with KeyValueCloudant, KeyValueFile, KeyValueGuava, KeyValueRedis, KeyValueSQL, KeyValueObjectStorage, and any other classes which implement the KeyValue interface.

The following method call adds 42 to the data store indexed by "key1":
~~~ java
    datastore.put(key1, 42);
~~~
The following method call adds all key-value pairs corresponding to "map" to the data store and returns the number of key-value pairs successfully stored:
~~~ java
    int numStored = datastore.putAll(map);
~~~
The following method call returns the value corresponding to "key".  It returns null if "key" is not found in the data store.
~~~ java
    val = datastore.get(key);
~~~
In the following method call, "list" is a list of keys.  getAll looks up all key-value pairs corresponding to keys in "list" and returns a map containing them.  In this example, keys are strings, and values are integers.
~~~ java
    Map<String, Integer> map = datastore.getAll(list);
~~~

The following method call deletes the key-value pair corresponding to "key2" from the data store if present: 
~~~ java
    datastore.delete(key2);
~~~
In the following method call, "list" is a list of keys.  deleteAll deletes all key-value pairs corresponding to a key in "list".  The number of deleted key-value pairs is returned:
~~~ java
    numDeleted = datastore.deleteAll(list);
~~~

The following method call deletes all objects in the data store:
~~~ java
    datastore.clear();
~~~

The following method call outputs a string identifying the type of data store represented by "datastore":
~~~ java
    datastore.storeType();
~~~

The size method returns the number of objects in the data store:
~~~ java
    System.out.println("Data store size: " + datastore.size());
~~~

The following displays the contents of the entire data store.  It should not be invoked if the data store contains a large amount of data as the data outputted would be prohibitively large:
~~~ java
    System.out.println(datastore.toString());
~~~
