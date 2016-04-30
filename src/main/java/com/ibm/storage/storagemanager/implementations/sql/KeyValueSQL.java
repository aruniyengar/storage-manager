package com.ibm.storage.storagemanager.implementations.sql;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.ibm.storage.storagemanager.util.Constants;
import com.ibm.storage.storagemanager.util.Serializer;
import com.ibm.storage.storagemanager.util.Util;

import static com.ibm.storage.storagemanager.util.Constants.NUM_UNKNOWN;

/**
 * @author ArunIyengar
 * 
 */
public class KeyValueSQL<K,V> implements com.ibm.storage.storagemanager.interfaces.KeyValue<K, V> {

    private static final String COLUMN1_NAME = "id_string";  // name of 1st column in database
    private static final String COLUMN2_NAME = "value_byte_array";  // name of 2nd column in database
    
    private static final int MAX_KEY_SIZE = 200;  // maximum length of a key

    private String baseUrl;
    private String dbName;
    private String dbUrl;
    private String password;
    private String tableName;
    private String userId;

    private Connection rootConnection = null;
    private Connection dbConnection = null;

    /**
     * Constructor. Establishes a session with a Key-value store, reading in credentials from a file.
     * 
     * @param tableName
     *            identifies the table name for the key-value pairs
     * @param inputFile
     *            Name of file storing database name, database URL, user id, and password
     * @param clearAll
     *            true if all previous table entries should be deleted
     */
    public KeyValueSQL(String tblName, String inputFile, boolean clearAll) {
        File file = new File(inputFile);
        Scanner input = null;
        try {
            input = new Scanner(file);
        } catch (FileNotFoundException e) {
            System.out.println("Error in SQL KeyValue consstructor: File " + inputFile + " not found.  Exiting.");
            System.exit(1);
        }
        dbName = Util.getNextWord(input);
        baseUrl = Util.getNextWord(input);
        dbUrl = baseUrl + dbName;
        userId = Util.getNextWord(input);
        password = Util.getNextWord(input);
        tableName = tblName;
        initialize(clearAll);
    }
    
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
            boolean clearAll) {
        dbName = databaseName;
        baseUrl = url;
        dbUrl = baseUrl + dbName;
        userId = userName;
        password = passWord;
        tableName = tblName;
        initialize(clearAll);
    }
        
    private void initialize(boolean clearAll) {
        /*
         * Following is only required before JDBC version 4
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Error: MySQL JDBC Driver not found");
            e.printStackTrace();
            return;
        }
        System.out.println("MySQL JDBC Driver Registered");
        */
        rootConnection = openConnection(baseUrl);
        if (rootConnection == null) {
            System.out.println("Error: could not connect to " + baseUrl);
            return;
        }
        createDatabase(dbName);
        dbConnection = openConnection(dbUrl);
        if (dbConnection == null) {
            System.out.println("Error: could not connect to " + dbUrl);
            return;
        }
        System.out.println("Return value from createTable: " + createTable(tableName, clearAll));
        printDatabaseInfo();
        
    }
    
    private void printDatabaseInfo() {
        printJdbcVersion();
        listDatabases();
        listTables();        
    }
    
    private  DatabaseMetaData getMetaInfo() {
        DatabaseMetaData meta = null;
        try {
            meta = dbConnection.getMetaData();
        } catch (SQLException e) {
            System.out.println("Error: getMetaData in getMetaInfo Failed.");
            e.printStackTrace();
        }
        return meta;
    }
    
    private void listDatabases() {
        System.out.println("Catalog Content");
        try {
            DatabaseMetaData dbmd = rootConnection.getMetaData();
            try (ResultSet ctlgs = dbmd.getCatalogs()) {             
                while (ctlgs.next()) {
                    System.out.println(ctlgs.getString(1));
                }
            }
            System.out.println();           
        } catch (SQLException e) {            
        }
    }
    
    private void printJdbcVersion() {
        try {
            DatabaseMetaData meta = rootConnection.getMetaData();
            System.out.println("JDBC driver version is " + meta.getDriverVersion());        
        } catch (SQLException e) {
            System.out.println("Error: getMetaData in printJdbcVersion Failed.");
            e.printStackTrace();
        }
    }
    
    private Connection openConnection(String url) {
        try {
            Connection connection = DriverManager.getConnection(url, userId, password);
            return connection;
        } catch (SQLException e) {
            System.out.println("Error: openConnection to " + url + " Failed.");
            e.printStackTrace();
            return null;
        }
    }
    
    // Reset connections, if needed
    public boolean resetConnections() {
        closeConnections();
        rootConnection = openConnection(baseUrl);
        if (rootConnection == null) {
            System.out.println("Error: could not connect to " + baseUrl);
            return false;
        }
        dbConnection = openConnection(dbUrl);
        if (dbConnection == null) {
            System.out.println("Error: could not connect to " + dbUrl);
            return false;
        }
        return true;
    }
    
    public boolean closeConnections() {
        try {
            rootConnection.close();
            dbConnection.close();
            return true;
        } catch (SQLException se) {
            System.out.println("Error: in closeConnections: Connection failed to close");
            se.printStackTrace();
            return false;
        }

    }
    
    private static boolean executeStatement(String sql, Connection connection) {
        try {
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.executeUpdate();
                return true;
            }
        } catch (SQLException se) {
            System.out.println("Error.  createDatabase "+ sql + " failed.");
            se.printStackTrace();
            return false;
        }
        
    }
    
    public boolean createDatabase(String dbName) {
        return executeStatement ("CREATE DATABASE IF NOT EXISTS " + dbName, rootConnection);        
    }
    
    public boolean dropDatabase(String dbName) {
        return executeStatement ("DROP DATABASE IF EXISTS " + dbName, rootConnection);
    }

    public boolean dropTable(String tblName) {
        return executeStatement("DROP TABLE " + tblName, dbConnection);
    }
    
    // create tblName if it does not exist.  If it exists, remove all entries iff
    // removeAll == true
    // return true if there are no sql exceptions caught
    public boolean createTable(String tblName, boolean removeAll) {
        try {
            try (ResultSet tables = getMetaInfo().getTables(null, null, tblName, null)) {
                // ResultSet tables = getMetaInfo().getTables(null, null, tblName, null);
                if (!tables.next()) {  // if table does not exist
                    String sql = "CREATE TABLE " + tblName + 
                            " (" + COLUMN1_NAME + " VARCHAR(" + MAX_KEY_SIZE + ") NOT NULL," +
                            " " + COLUMN2_NAME + " LONGBLOB, " +
                            "PRIMARY KEY (" + COLUMN1_NAME + "))";
                    return executeStatement(sql, dbConnection);
                }
                else if (removeAll) {
                    if (clear() == ReturnStatus.SUCCESS) {
                        return true;
                    }
                    else {
                        return false;
                    }
                }
                else {
                    return true;
                }
            }
        } catch (SQLException se) {
            System.out.println("Error.  createTable " + tblName + " failed.");
            se.printStackTrace();
            return false;
        }
    }
    

    
    public void listTables() {
        System.out.println("Tables in database " + dbName);
        try {
            try (ResultSet rs = getMetaInfo().getTables(null, null, "%", null)) {
                while (rs.next()) {
                    System.out.println(rs.getString(3));
                }
            }
        } catch (SQLException se) {
            System.out.println("Error.  listTables failed.\n");
            se.printStackTrace();
        }
        System.out.println();
    }

    public void displayTable() {
        System.out.println("Contents of table " + tableName);        
        String sql = "SELECT * FROM " + tableName;
        try {
            try (PreparedStatement pstmt = dbConnection.prepareStatement(sql)) {
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString(COLUMN1_NAME);
                        byte[] array = rs.getBytes(COLUMN2_NAME);
                        V value = Serializer.deserializeFromByteArray(array);
                        System.out.println(key + ", " + value);
                    }
                }
            }
        }  catch (SQLException se) {
            System.out.println("Error.  listTables failed.\n");
            se.printStackTrace();
        }
        System.out.println("End of table " + tableName + "\n");        
    }
    
    
    /**
     * delete all entries from the storage service
     * 
     * @return status code
     * 
     * */
   @Override
   public ReturnStatus clear() {
       String sql = "DELETE FROM " + tableName;
       try {
           try (PreparedStatement pstmt = dbConnection.prepareStatement(sql)) {
               pstmt.executeUpdate();
               return ReturnStatus.SUCCESS;
           }
       }  catch (SQLException se) {
           System.out.println("Error.  clear failed.\n");
           se.printStackTrace();
           return ReturnStatus.FAILURE;
       }
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
        String sql = "DELETE FROM " + tableName +
                " WHERE " + COLUMN1_NAME + " = ?";
        try {
            try (PreparedStatement pstmt = dbConnection.prepareStatement(sql)) {
                pstmt.setString(1, (String) key);
                return pstmt.executeUpdate();
            }
        }  catch (SQLException se) {
            System.out.println("Error.  clear failed.\n");
            se.printStackTrace();
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
    // Could make this more efficient using DELETE FROM table WHERE id IN (?,?,?,?,?,?,?,?)
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
        String sql = "SELECT " + COLUMN2_NAME + " FROM " + tableName +
                " WHERE " + COLUMN1_NAME + " = ?";
        try {
            try (PreparedStatement pstmt = dbConnection.prepareStatement(sql)) {
                pstmt.setString(1, (String) key);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        byte[] array = rs.getBytes(1);
                        V value = Serializer.deserializeFromByteArray(array);
                        return value;
                    }
                    else {
                        return null;
                    }
                }
            }
        }  catch (SQLException se) {
            System.out.println("Error.  get failed.\n");
            se.printStackTrace();
            return null;
        }
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
     * Return database connection for applications to explicitly use.
     * 
     * @return connection to database
     * 
     * */
    public Connection getConnection() {
        return dbConnection;
    }

    /**
     * Return a string idenfitying the type of storage service
     * 
     * @return string identifying the type of storage service
     * */
    @Override
    public String storeType() {
        return Constants.SQLID;
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
        String sql = "INSERT " + tableName + 
                " (" + COLUMN1_NAME + ", " + COLUMN2_NAME + ") VALUES (?, ?) " + 
                "ON DUPLICATE KEY UPDATE " +
                COLUMN2_NAME + " = VALUES(" + COLUMN2_NAME + ")";
        String keyString = (String) key;
        byte[] array = Serializer.serializeToByteArray(value);
        try {
            
            try (PreparedStatement pstmt = dbConnection.prepareStatement(sql)) {
                pstmt.setString(1, keyString);
                pstmt.setBytes(2, array);
                pstmt.executeUpdate();
                return ReturnStatus.SUCCESS;
            }
        } catch (SQLException se) {
            System.out.println("Error.  put failed: "+ sql);
            se.printStackTrace();
            return ReturnStatus.FAILURE;
        }
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
        String sql = "SELECT COUNT(*) FROM " + tableName;
        try {
            try (PreparedStatement pstmt = dbConnection.prepareStatement(sql)) {
                try (ResultSet rs = pstmt.executeQuery()) {
                    rs.next();
                    return rs.getInt(1);
                }
            }
        }  catch (SQLException se) {
            System.out.println("Error.  size failed.\n");
            se.printStackTrace();
        }
        return NUM_UNKNOWN;
    }

    /**
     * Return contents of entire cache in a string
     * 
     * @return string containing output
     * 
     * */
    @Override
    public String toString() {
        String returnVal = "Contents of table " + tableName + "\n";        
        String sql = "SELECT * FROM " + tableName;
        try {
            try (PreparedStatement pstmt = dbConnection.prepareStatement(sql)) {
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        String key = rs.getString(COLUMN1_NAME);
                        byte[] array = rs.getBytes(COLUMN2_NAME);
                        V value = Serializer.deserializeFromByteArray(array);
                        returnVal += key + ", " + value + "\n";
                    }
                }
            }
        }  catch (SQLException se) {
            System.out.println("Error.  listTables failed.\n");
            se.printStackTrace();
        }
        returnVal += "End of table " + tableName + "\n\n";        
        return returnVal;
    }
    
    public static void test1(KeyValueSQL<String, Integer> os1) {
        String key1 = "key1";
        String key2 = "key2";
        int val1 = 7;
        int val2 = 90;
        
        System.out.println("Table before any changes\n" + os1.toString());
        os1.put(key1, val1);
        os1.put(key2, val2);
        os1.put(key1, val2);
        os1.put(key2, val1);
        System.out.println("Table after 2 insertions and 2 updates\n" + os1.toString());
        System.out.println("Fetched value of " + key1 + " is " + os1.get(key1));
        System.out.println(os1.toString());
        
        System.out.println("Value of delete " + key1 + ": " + os1.delete(key1));
        System.out.println("Value of delete " + key1 + " a 2nd time: " + os1.delete(key1));
        System.out.println("Table before key2 deleted: " + os1.toString());
        System.out.println("Value of delete " + key2 + ": " + os1.delete(key2));

        System.out.println("Table after both deletions\n" + os1.toString());
        
        System.out.println("Connections closed: " + os1.closeConnections());
        System.out.println("Connections closed: " + os1.closeConnections());        
    }
 
    public static void main(String[] args) {
        String configFile = Util.configFile(Constants.SQLID);
        String tblName = "test1";
        KeyValueSQL<String, Integer> os1 = new KeyValueSQL<String, Integer>(tblName, configFile, true);
        test1(os1);
        System.out.println(os1.storeType());
        System.out.println("Main finished executing");

    }
    
}
