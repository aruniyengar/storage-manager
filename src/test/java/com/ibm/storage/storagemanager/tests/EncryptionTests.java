/**
 * 
 */
package com.ibm.storage.storagemanager.tests;



import java.util.HashMap;

import javax.crypto.SealedObject;

import org.junit.Test;

import com.ibm.storage.storagemanager.util.Encryption;

/**
 * @author ArunIyengar
 *
 */
public class EncryptionTests {

    @Test
    public void testEncryption() {
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        HashMap<String, Integer> hm = new HashMap<String, Integer>();
        hm.put(key1, 22);
        hm.put(key2, 23);
        hm.put(key3, 24);
        System.out.println("original hash table: " + hm);
        Encryption.Key secretKey = Encryption.generateKey();
        SealedObject so = Encryption.encrypt(hm, secretKey);
        HashMap<String, Integer> hm2 = Encryption.decrypt(so, secretKey);
        System.out.println("Decrypted hash table: " + hm2);
  }

}
