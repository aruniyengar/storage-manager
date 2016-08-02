/**
 * 
 */
package com.ibm.storage.storagemanager.performancetester;

import com.ibm.storage.storagemanager.implementations.file.KeyValueFile;
import com.ibm.storage.storagemanager.implementations.guava.KeyValueGuava;
import com.ibm.storage.storagemanager.implementations.redis.KeyValueRedis;
import com.ibm.storage.storagemanager.interfaces.KeyValue;
import com.ibm.storage.storagemanager.util.Compression;
import com.ibm.storage.storagemanager.util.Constants;
import com.ibm.storage.storagemanager.util.Encryption;
import com.ibm.storage.storagemanager.util.Util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.Scanner;

import javax.crypto.SealedObject;

import org.apache.commons.io.FileUtils;


/**
 * @author ArunIyengar
 * This class tests performance of different storage allocators
 *
 */
public class PerformanceTester {
    
    private enum DataGenerator {
        ZERO, RANDOM, FILE, USER_DEFINED
    }
    
    private enum Operations {
        READ, WRITE, COMPRESS, DECOMPRESS, ENCRYPT, DECRYPT
    }

    
    private final static boolean DEBUG_MODE = true;
    private final static Charset CHARSET = Charset.forName("US-ASCII");
    private final static String FILENAME_COMPRESSION = "compression"; // compression and decompression performance data 
    private final static String FILENAME_ENCRYPTION = "encryption"; // encryption and decryption performance data 
    private final static String FILENAME_SUFFIX_FAST_CACHE = "_fast_cache"; // performance data with fast cache 
    private final static String FILENAME_SUFFIX_NO_CACHE = "_no_cache"; // performance data without a cache 
    private final static String FILENAME_SUFFIX_REMOTE_CACHE = "_remote_cache"; // performance data with remote cache 
    private final static String KEY_ROOT = "key"; // beginning of key for each object in data store 
    private final static String LINE_SEPARATOR = System.getProperty("line.separator");
    private final static String SEPARATOR = "#"; // to separate comments from input values
                                                 // in input files
    
    private byte[][] compressedObjects; 
    private double[] compressionTimes;
    private DataGenerator dataType;  // indicates type of data input
    private double[] decompressionTimes;
    private double[] decryptionTimes;
    private SealedObject[] encryptedObjects; 
    private double[] encryptionTimes;
    private KeyValueGuava<String, byte[]> fastCache;
    private double[] fastCacheHitTimes;
    private double fastCacheMissTime;
    private int hitRates;   // # of hit rates to test
    private String inputFile;  // file containing input for object data
    Encryption.Key secretKey = Encryption.generateKey();
    private double latencyDivisor; // latencies divided by this number, allows scaling for graphs
    private int maxObjectSize;   // maximum object size
    private int objectSizes;   // # of object sizes to test
    private String outputDirectory;  // directory where output files are stored
    private byte[][] objects;     // objects used for the performance tests
    private KeyValueRedis<String, byte[]> remoteCache;
    private double[] remoteCacheHitTimes;
    private double remoteCacheMissTime;
    private int runs;   // # of times to run test corresponding to a single data point
    private double sizeDivisor; // sizes divided by this number, allows scaling for graphs
    private boolean useRemoteCache;  // true if a remote process cache should be used,
                                     // in addition to the inprocess cache
    
    private PerformanceTester(String configFile) throws Exception {
        readConfigInfo(configFile);
        objects = new byte[objectSizes][];
        compressedObjects = new byte[objectSizes][];
        encryptedObjects = new SealedObject[objectSizes];
        switch (dataType) {
        case ZERO:
            objectsZero();
            break;
        case RANDOM:
            objectsRandom();
            break;
        case FILE:
            objectsFile();
            break;
        case USER_DEFINED:
            objects = getTestData();
            break;
        default:
            Util.throwException("Error in PerformanceTester constructor: illegal option for creating data: " + dataType);
        }
        testCache();
        testCompression();
        testEncryption();
        outputResults();
    }
    
    private void testCompression() throws Exception {
        compressionTimes = runTests(null, Operations.COMPRESS);
        decompressionTimes = runTests(null, Operations.DECOMPRESS);
    }
    
    private void testEncryption() throws Exception {
        encryptionTimes = runTests(null, Operations.ENCRYPT);
        decryptionTimes = runTests(null, Operations.DECRYPT);
    }
    
    private void testCache() throws Exception {
        fastCache = new KeyValueGuava<String, byte[]>(objectSizes + 10);
        populateStore(fastCache);
        fastCacheHitTimes = runTests(fastCache, Operations.READ);
        fastCacheMissTime = missTime(fastCache);
        System.out.println("Fast cache miss time in nanosecods is: " + fastCacheMissTime);
        if (useRemoteCache) {
            remoteCache = new KeyValueRedis<String, byte[]>("localhost", 6379, 60);
            populateStore(remoteCache);
            remoteCacheHitTimes = runTests(remoteCache, Operations.READ);
            remoteCacheMissTime = missTime(remoteCache);
            System.out.println("Remote cache miss time in nanosecods is: " + remoteCacheMissTime);
        }
    }
    
    /*
     * Populate a data store.  This is needed before doing meaningful lookup tests
     */
    private void populateStore(KeyValue<String, byte[]> dataStore){
        for (int index = 0; index < objectSizes; index++) {
            dataStore.put(KEY_ROOT + index, objects[index]);
        }
    }
    
    /*
     * Run tests over all request sizes
     */
    private double[] runTests(KeyValue<String, byte[]> dataStore, Operations operation) throws Exception {
        double[] times = new double[objectSizes];
        for (int i = 0; i < objectSizes; i++) {
            double totalTime = 0.0;
            for (int j = 0; j < runs; j++) {
                totalTime += timeRequest(dataStore, operation, i);
            }
            times[i] = totalTime/runs;
        }
        return times;
    }
    
    /*
     * Return the time to run a read (get) or write (put) request on a particular data store
     */
    private double timeRequest(KeyValue<String, byte[]> dataStore, Operations operation, int index) 
        throws Exception {
        long startTime = 0;
        long endTime = 0;
        switch(operation) {
            case READ:
                startTime = System.nanoTime();
                byte[] returnVal = dataStore.get(KEY_ROOT + index);
                endTime = System.nanoTime();
                if (returnVal.length != objects[index].length) {
                    Util.throwException("Error in PerformanceTester.timeRequest.  Returned object of length "
                            + returnVal.length + " does not match expected length: " + objects[index].length);
                }
                break;
            case WRITE:
                startTime = System.nanoTime();
                dataStore.put(KEY_ROOT + index, objects[index]);
                endTime = System.nanoTime();           
                break;
            case COMPRESS:
                startTime = System.nanoTime();
                compressedObjects[index] = Compression.compress(objects[index]);
                endTime = System.nanoTime();           
                break;
            case DECOMPRESS:
                startTime = System.nanoTime();
                Compression.decompress(compressedObjects[index]);
                endTime = System.nanoTime();           
                break;
            case ENCRYPT:
                startTime = System.nanoTime();
                encryptedObjects[index] = Encryption.encrypt(objects[index], secretKey);
                endTime = System.nanoTime();           
                break;
            case DECRYPT:
                startTime = System.nanoTime();
                Encryption.decrypt(encryptedObjects[index], secretKey);
                endTime = System.nanoTime();           
                break;
            default:
                Util.throwException("Error in PerformanceTester.timeRequest: illegal Operation: " + operation);
       }
        return endTime - startTime;
    }
    
    /*
     * Determine the miss time for a data store
     */
    private double missTime(KeyValue<String, byte[]> dataStore) throws Exception {
        double totalTime = 0.0;
        for (int j = 0; j < runs; j++) {
            double startTime = System.nanoTime();
            byte[] returnVal = dataStore.get("adfj");
            double endTime = System.nanoTime();
            if (returnVal != null) {
                Util.throwException("Error in PerformanceTester.missTime.  Object should not be in data store. "
                        + " Length of returned object is: " + returnVal.length);
            }
            totalTime += (endTime - startTime);
        }
        return totalTime/runs;
    }
    
    private static double readDivisor (Scanner input) throws Exception {
        double value = Double.parseDouble(Util.getNextInput(input));
        if (value <= 0.0) {
            Util.throwException("Error.  Illegal scaling divisor: " + value);
        }
        return value;
    }
    
    private void readConfigInfo(String configFile) throws Exception {
        System.out.println("Configuration file: " + configFile);
        File file = new File(configFile);
        Scanner input = null;
        try {            
            input = new Scanner(file);
        } catch (FileNotFoundException e) {
            System.out.println("Error in PerformanceTester constructor: File " +
                    configFile + " not found.  Exiting.");
            System.exit(1);
        }
        outputDirectory = Util.getNextInput(input);
        if (outputDirectory.charAt(outputDirectory.length() - 1) != (File.separatorChar)) {
            outputDirectory += File.separatorChar;
        }
        maxObjectSize = Util.readPositiveInt("maximum object size", input);
        objectSizes = Util.readPositiveInt("# of object sizes", input);
        if (objectSizes > maxObjectSize) {
            objectSizes = maxObjectSize;
        }
        hitRates = Integer.parseInt(Util.getNextInput(input));
        if (hitRates < 2) {
            Util.throwException("Error.  Illegal number of hit rates: " + hitRates);
            
        }
        int useRemoteProcessCache = Integer.parseInt(Util.getNextInput(input));
        if (useRemoteProcessCache == 1) {
            useRemoteCache = true;
        }
        runs = Util.readPositiveInt("# of test runs per data point", input);
        sizeDivisor = readDivisor(input);
        latencyDivisor = readDivisor(input);
        int type = Integer.parseInt(Util.getNextInput(input));
        switch (type) {
            case 0:
                dataType = DataGenerator.ZERO;
                break;
            case 1:
                dataType = DataGenerator.RANDOM;
                break;
            case 2: 
                dataType = DataGenerator.FILE;
                break;
            default:
                Util.throwException("Configuration parameter error: illegal parameter"
                        + " specifying how to generate data (shoud be 1 or 2): " + type);
                break;
        }
        if (dataType == DataGenerator.FILE) {
            inputFile = Util.getNextInput(input);
        }
    }
    
    private void objectsZero() {
        for (int i = 0; i < objectSizes; i++) {
            objects[i] = new byte[maxObjectSize * (i + 1)/objectSizes];
        }
    }
    
    private void objectsRandom() {
        Random rand = new Random(); 
        for (int i = 0; i < objectSizes; i++) {
            int numBytes = maxObjectSize * (i + 1)/objectSizes;
            objects[i] = new byte[numBytes];
            for (int j = 0; j < numBytes; j++) {
                objects[i][j] = (byte) (rand.nextInt(256) - 128);
            }
        }
    }
    
    private void objectsFile() throws Exception {
        try (FileInputStream inputStream = new FileInputStream(inputFile)) {
            for (int i = 0; i < objectSizes; i++) {
                int numBytes = maxObjectSize * (i + 1)/objectSizes;
                objects[i] = new byte[numBytes];
                int bytesRead = inputStream.read(objects[i]);
                if (bytesRead < numBytes) {
                    Util.throwException("Error in PerformanceTester.objectsFile: ran out of data for object "
                            + i);
                }
            }
        } catch (Exception e) {
            System.out.println("Error in PerformanceTester.objectsFile: cannot read data objects from file: "
                    + inputFile);
            throw e;
        }
    }

    /**
     * User-defined function for getting objects to test storage system.
     * 
     * @return array storing byte arrays to test storage systems
     */
    public byte[][] getTestData() {
        return null;
    }
    
    private void printObjects() {
        System.out.println("Object data");
        for (int i = 0; i < objectSizes; i++) {
            System.out.println("Index: " + i + " length: " + objects[i].length);
            System.out.println(Arrays.toString(objects[i]));
            objects[i] = new byte[maxObjectSize * i/objectSizes];
        }
    }
    
    public void outputTestParameters() {
        System.out.println("Output directory: " + outputDirectory);
        System.out.println("Maximum object size: " + maxObjectSize);
        System.out.println("# of object sizes: " + objectSizes);
        System.out.println("# of hit rates: " + hitRates);
        System.out.println("Use remote cache: " + useRemoteCache);
        System.out.println("# of runs per data point: " + runs);
        System.out.println("latency divisor: " + latencyDivisor);
        System.out.println("size divisor: " + sizeDivisor);
        System.out.println("type of data: " + dataType);
        System.out.println("input file for data: " + inputFile);
    }
    
    /**
     * Run all tests, store the results in output files.
     * 
     * @param configFile
     *            file storing configuration parameters
     * @param dataStores
     *            Array containing all data stores to test
     */
    public static void runAllTests(String configFile, ArrayList<KeyValue<String, byte[]>> dataStores) throws Exception {
        PerformanceTester pt = new PerformanceTester(configFile);
        if (DEBUG_MODE) {
            pt.outputTestParameters();
            // pt.printObjects();
        }
        for (KeyValue<String, byte[]> dataStore : dataStores) {
            pt.testDataStore(dataStore);
        }
        System.out.println("All tests finished.  PerformanceTester.runAllTests exiting");
    }
    
    /*
     * Output results independent of specific data stores, such as compression & encryption performance
     */
    private void outputResults() throws Exception {
        deleteDirectory(outputDirectory);
        Files.createDirectory(Paths.get(outputDirectory));        
        outputTimePairs(Paths.get(outputDirectory + FILENAME_COMPRESSION), compressionTimes,
                decompressionTimes, "# 1st column: object sizes.  2nd column: " + "compression latencies."
                        + "  3rd column: decompression latencies" + LINE_SEPARATOR);
        outputTimePairs(Paths.get(outputDirectory + FILENAME_ENCRYPTION), encryptionTimes,
                decryptionTimes, "# 1st column: object sizes.  2nd column: " + "encryption latencies.  "
                        + "3rd column: decryption latencies" + LINE_SEPARATOR);
    }
    
    private static void deleteDirectory(String directory) {
        File fileHdl = new File(directory);
        try {
            FileUtils.deleteDirectory(fileHdl);
        }
        catch (Exception e) {
            System.out.println("Directory " + directory + " could not be deleted");
        }
    }
    
    private Path createFile(String fileName) throws Exception {
        Path path = Paths.get(fileName);
        Files.createFile(path);
        return path;
    }
    
    private void outputDataWithCache(Path path, double[] readTimes, double[] cacheHitTimes, 
            double cacheMissTime) throws Exception {
        try (BufferedWriter writer = Files.newBufferedWriter(path, CHARSET)) {
            writer.write("# 1st column: object sizes.  remaining columns: latencies, hit rate multiples of "
                    + (double)   1 / (hitRates - 1) + " start hit rate: 0, end hit rate 1, "
                    + runs + " data points" + LINE_SEPARATOR);
            writeDivisors(writer);
            for (int i = 0; i < objectSizes; i++) {
                String formatStr = "%f";
                writer.write(String.format(formatStr, objects[i].length/sizeDivisor));
                for (int j = 0; j < hitRates; j++) {
                    double hitRate = (double) j / (hitRates - 1);  
                    double latency = (readTimes[i] + cacheMissTime) * (1 - hitRate) + 
                            cacheHitTimes[i] * hitRate;
                    formatStr = " %f";
                    writer.write(String.format(formatStr, latency/latencyDivisor));
                }
                writer.write(LINE_SEPARATOR);
            }
        }
        
    }
    
    private void outputTimePairs(Path path, double[] times1, double[] times2, String header) throws Exception {
        Files.createFile(path);
        try (BufferedWriter writer = Files.newBufferedWriter(path, CHARSET)) {
            writer.write(header);
            writeDivisors(writer);
            for (int i = 0; i < objectSizes; i++) {
                String formatStr = "%f %f %f%n";
                writer.write(String.format(formatStr, objects[i].length/sizeDivisor, 
                        times1[i]/latencyDivisor, times2[i]/latencyDivisor));
            }
        }        
    }
    
    /*
     * Run tests on a single data store
     */
    private void testDataStore(KeyValue<String, byte[]> dataStore) throws Exception {
        double[] writeTimes = runTests(dataStore, Operations.WRITE);
        double[] readTimes = runTests(dataStore, Operations.READ);
        String filePrefix = outputDirectory + dataStore.storeType();
        outputTimePairs(Paths.get(filePrefix+ FILENAME_SUFFIX_NO_CACHE), readTimes, writeTimes, "# 1st column: object sizes.  2nd column: " +
                "read latencies without caching.  3rd column: write latencies without caching, "
                + runs + " data points" + LINE_SEPARATOR);
        outputDataWithCache(createFile(filePrefix + FILENAME_SUFFIX_FAST_CACHE), readTimes,
                fastCacheHitTimes, fastCacheMissTime);
        if (useRemoteCache) {
            outputDataWithCache(createFile(filePrefix + FILENAME_SUFFIX_REMOTE_CACHE), readTimes,
                    remoteCacheHitTimes, remoteCacheMissTime);
        }
    }
    
    private void writeDivisors(BufferedWriter writer) throws Exception {
        writer.write("# Object sizes: bytes divided by " + sizeDivisor + LINE_SEPARATOR);
        writer.write("# Latencies: nanoseconds divided by " + latencyDivisor + LINE_SEPARATOR);
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        String configFile = Util.configFile(Constants.PERFORMANCETEST);
        KeyValueFile<String, byte[]> datastore = new KeyValueFile<String, byte[]>("db1", true);
        
        ArrayList<KeyValue<String, byte[]>> dataStores = new ArrayList<KeyValue<String, byte[]>>();
        dataStores.add(datastore);
        runAllTests(configFile, dataStores);
    }

}
