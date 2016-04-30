package com.ibm.storage.storagemanager.util;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;

/**
 * @author ArunIyengar
 *
 */
public class Serializer {

    /**
     * Deserialize a single object from a byte array
     * 
     * @param bytes
     *            bytes to deserialize
     * @param <T>
     *            type of deserialized object
     * @return deserialized object
     * 
     * */
    public static <T> T deserializeFromByteArray(byte[] bytes) {
        if ((bytes == null) || (bytes.length == 0)) {
            return null;
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = null;
        T r = null;
        try {
            in = new ObjectInputStream(bis);
            r = Util.uncheckedCast(in.readObject());
        } catch (IOException i) {
            System.out.println("Exception in Serializer.deserializeFromByteArray  " + ",  " + i.getMessage());
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out
                    .println("Serializer.deserializeFromByteArray: class not found");
            c.printStackTrace();
            return null;
        } finally {
            try {
                bis.close();
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return r;
    }

    /**
     * Serialize a single object to a byte array
     * 
     * @param r
     *            object to serialize
     * @param <T>
     *            type of deserialized object
     * @return serialized object
     * 
     * */
    public static <T> byte[] serializeToByteArray(T r) {
        if (r == null) {
            return new byte[0];
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        byte[] bytes = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(r);
            bytes = bos.toByteArray();
        } catch (IOException ex) {
            System.out.println("Exception in Serializer.serializeToByteArray  " + ",  " + ex.getMessage() + " "
                    + ex.getStackTrace());
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return bytes;
    }


    /**
     * Deserialize a string from a byte array
     * 
     * @param bytes
     *            bytes to deserialize
     * @param charset
     *            string character set
     * @return deserialized object
     * 
     * */
    public static String deserializeToString(byte[] bytes, Charset charset) {
        return (bytes == null ? null : new String(bytes, charset));
    }

    /**
     * Serialize a string to a byte array
     * 
     * @param string
     *            string to serialize
     * @param charset
     *            string character set
     * @return serialized string
     * 
     * */
    public static byte[] serializeString(String string, Charset charset) {
        return (string == null ? null : string.getBytes(charset));
    }
    
    
}

