package com.ibm.storage.storagemanager.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


/**
 * @author ArunIyengar 
 * 
 */
/*
 * Methods to compress and decompress data
 */
public class Compression {

    /**
     * compress a serializable object using gzip
     * 
     * @param object
     *            object which implements Serializable
     * @return byte array containing compressed objects
     * 
     * */
    public static byte[] compress(Serializable object) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            GZIPOutputStream gzipOut = null;
            ObjectOutputStream objectOut = null;
            try {
                gzipOut = new GZIPOutputStream(baos);
                objectOut = new ObjectOutputStream(gzipOut);
                objectOut.writeObject(object);
            }
            catch (Exception e) {
                Util.describeException(e, "Exception in Util.compress");
            }
            finally {
                objectOut.close();
                gzipOut.close();
            }
        }
        catch (Exception e) {
            Util.describeException(e, "Exception in Util.compress");
        }
        return baos.toByteArray();
    }

    /**
     * Decompress a compressed object
     * 
     * @param bytes
     *            byte array corresponding to compressed object
     * @param <T>
     *            type of decompressed object
     * @return decompressed object
     * 
     * */
    public static <T> T decompress(byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        T object = null;
        try {
            GZIPInputStream gzipIn = null;
            ObjectInputStream objectIn = null;
            try  {
                gzipIn = new GZIPInputStream(bais);
                objectIn = new ObjectInputStream(gzipIn);
                object = Util.uncheckedCast(objectIn.readObject());
            }
            catch (Exception e) {
                Util.describeException(e, "Exception in Util.decompress");
            }
            finally {
                objectIn.close();
                gzipIn.close();
            }       
        }
        catch (Exception e) {
            Util.describeException(e, "Exception in Util.decompress");
        }
        return object;
    }

    
}
