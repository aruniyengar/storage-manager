package com.ibm.storage.storagemanager.tests;

import java.util.HashMap;

import org.junit.Test;

import com.ibm.storage.storagemanager.util.Serializer;
import com.ibm.storage.storagemanager.util.Compression;

public class CompressionTests {

    @Test
    public void testCompression() {
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        HashMap<String, Integer> hm = new HashMap<String, Integer>();
        hm.put(key1, 22);
        hm.put(key2, 23);
        hm.put(key3, 24);
        System.out.println("original hash table: " + hm);
        byte[] compressed = Compression.compress(hm);
        HashMap<String, Integer> hm2 = Compression.decompress(compressed);
        System.out.println("Hash table after compression and decompression: " + hm2);
        byte[] serialized = Serializer.serializeToByteArray(hm);
        System.out.println("Compressed object size: " + compressed.length);
        System.out.println("Serialized object size: " + serialized.length);
    }

    
}
