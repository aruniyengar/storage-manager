/**
 * 
 */
package com.ibm.storage.storagemanager.implementations.cloudant;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @author ArunIyengar
 *
 * Implements storage for Cloudant objects
 */
public class DatabaseObject implements Serializable {
    private static final long serialVersionUID = 1L;
    private String _id;
    private String _rev;
    private byte[] value;

    DatabaseObject(String key, byte[] data) {
        _id = key;
        value = data;
    }

    public void setKey(String key) {
        _id = key;
    }

    public String getKey() {
        return _id;
    }

    public void setValue(byte[] data) {
        value = data;
    }

    public byte[] getValue() {
        return value;
    }

    public void print() {
        System.out.println("DatabaseObject ID: " + _id);
        System.out.println("version: " + _rev);
        System.out.println("value: " + Arrays.toString(value));
    }
    
    public String toString() {
        return "DatabaseObject ID: " + _id + "\n" + "version: " + _rev + "\n" +
                "value: " + Arrays.toString(value) + "\n";
    }

}
