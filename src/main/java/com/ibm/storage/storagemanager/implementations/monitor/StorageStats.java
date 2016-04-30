/**
 * 
 */
package com.ibm.storage.storagemanager.implementations.monitor;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import com.ibm.storage.storagemanager.util.Constants.RequestType;
import com.ibm.storage.storagemanager.util.Util;


/**
 * @author ArunIyengar
 * Statistics for an entire data store
 *
 */
public class StorageStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private RequestType[] requestTypes = RequestType.values();
    private String storeType;
    
    private AtomicLong startTime = new AtomicLong();  // start time for the monitoring interval
        // This is used to calculate average time per request but should not affect collection of
        // statistics
    private AtomicLong numRequests = new AtomicLong();  // total number of requests
    private AtomicLong totalRequestTime = new AtomicLong();  // total time spent processing requests
    private AtomicLong endTime = new AtomicLong();  // end time for the monitoring interval, might
        // not have been set yet.  This is used to calculate average time per request but should not 
        // affect collection of statistics
    private RequestStats[] requestStats = new RequestStats[requestTypes.length];
    
    
    /**
     * Constructor.
     * 
     * @param historySize
     *            # of most recent data points to keep around for each transaction type
     * @param type
     *            string identifying the type of store
     */
    public StorageStats(int historySize, String type) {
        storeType = type;
        startTime.set(Util.getTime());
        for (int i = 0; i < requestTypes.length; i ++) {
            requestStats[i] = new RequestStats(historySize, requestTypes[i].toString());
        }
    }
    
    // This method records a new data point
    void recordRequest(long timeInterval, RequestType requestType) {
        numRequests.incrementAndGet();
        totalRequestTime.addAndGet(timeInterval);
        requestStats[requestType.ordinal()].recordRequest(timeInterval);
    }
    
    /**
     * Return data structure containing data for a specific request type
     * 
     * @param requestType
     *            the request type
     * 
     * @return data structure for requestType
     * 
     * */
    public RequestStats getRequestData(RequestType requestType) {
        return requestStats[requestType.ordinal()];
    }
    
    /**
     * Return total number of requests
     * 
     * @return total number of requests
     * 
     * */
    public long getNumRequests() {
        return numRequests.get();
    }

    /**
     * Return total time taken by requests
     * 
     * @return total time taken by requests
     * 
     * */
    public long getTotalRequestTime() {
        return totalRequestTime.get();
    }
    
    /**
     * Return start time
     * 
     * @return start time
     * 
     * */
    public long getStartTime() {
        return startTime.get();
    }
    
    /**
     * Return end time
     * 
     * @return end time
     * 
     * */
    public long getEndTime() {
        return endTime.get();
    }
    
    /**
     * Return string indicating type of store corresponding to statistics
     * 
     * @return string indicating type of store corresponding to statistics
     * 
     * */
    public String getStoreType() {
        return storeType;
    }

/**
     * Set startTime to the current time.  startTime is used to calculate the total length of the monitoring
     * interval.  If this method is called after data points have already been collected, then the 
     * calculated total length of the monitoring interval and proportion of time spent in store
     * operations will not be accurate 
     * 
     * startTime is initialized to the time the StorageStats constructor is invoked.  Therefore,
     * setStartTimeNow() does not need to be invoked unless there is a delay before the StorageStats
     * object is used.
     * 
     * */
    public void setStartTimeNow() {
        startTime.set(Util.getTime());       
    }
        
    /**
     * Set endTime to the current time.  endTime is used to calculate total length of the monitoring
     * interval.  If data collection continues after endTime, then the calculated total length of the
     * monitoring interval and proportion of time spent in store operations will not be accurate.
     * 
     * */
    public void setEndTimeNow() {
        endTime.set(Util.getTime());       
    }

    /**
     * Return string describing summary statistics
     * 
     * @return string describing summary statistics
     * 
     * */
    public String summaryStats() {
        if (numRequests.get() == 0) {
            return "No requests for store " + storeType + "\n";
        }
        String returnVal = "Statistics for store of type " + storeType + "\n";
        returnVal += "Total requests: " + numRequests.get() + "\n";
        returnVal += "Total request time in milliseconds: " + totalRequestTime.get() + "\n";
        returnVal += "Average milliseconds per request: " +
               (((float) totalRequestTime.get())/numRequests.get()) + "\n";
        returnVal += "Start time in milliseocnds: " + startTime.get() + "\n";
        long timeEnd = endTime.get();
        if (timeEnd == 0) {
            timeEnd = Util.getTime();
        }
        returnVal += "End time in milliseconds: " + timeEnd + "\n";
        long intervalLength = timeEnd - startTime.get();
        returnVal += "Length of monitoring interval in milliseconds: " + intervalLength + "\n";
        returnVal += "Proportion of time spent on store operations: " + 
              (((float) totalRequestTime.get())/intervalLength) + "\n";
        return returnVal;
    }

    /**
     * Return string describing statistics by request type
     * 
     * @return string describing statistics by request type
     * 
     * */
    public String detailedStats() {
        if (numRequests.get() == 0) {
            return "No requests for store " + storeType + "\n";
        }
        String returnVal = "Statistics for all transaction types, store is: " + storeType + "\n";
        for (int i = 0; i < requestTypes.length; i ++) {
            returnVal += requestStats[i].allStats();                    
         }
        return returnVal;
    }

    /**
     * Return string describing all statistics
     * 
     * @return string describing all statistics
     * 
     * */
    public String allStats() {
        return summaryStats() + detailedStats();
    }

    
}
