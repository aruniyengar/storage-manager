/**
 * 
 */
package com.ibm.storage.storagemanager.util;

/**
 * @author ArunIyengar
 * Constants 
 *
 */
public final class Constants {
    // indicates that value of a nonnegative integer is unknown
    public static final int NUM_UNKNOWN = -1;
    
    // Following represent types of requests
    public enum RequestType{CLEAR, DELETE, DELETEALL, GET, GETALL,
        STORETYPE, PUT, PUTALL, SIZE, TOSTRING
    }

    // Following represent type of storage services supported   
    public static final String CLOUDANT = "cloudant";
    public static final String FILE = "file";
    public static final String GUAVA = "guava";
    public static final String OBJECTSTORAGE = "objectstorage";
    public static final String REDIS = "redis";
    public static final String SQLID = "sql";

    public static final String PERFORMANCETEST = "performancetest";

    
    // location of configuration files
    public static final String CONFIG_DIRECTORY = "config";

    // configuration file names end in this string
    public static final String CONFIG_FILE_SUFFIX = ".config";

    
}
