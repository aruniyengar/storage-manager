/**
 * 
 */
package com.ibm.storage.storagemanager.implementations.monitor;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author ArunIyengar
 * Statistics for a particular type of requests
 *
 */
public class RequestStats implements Serializable {
    private static final long serialVersionUID = 1L;

    private long[] requestTimes;   // maintain request times for last n requests
    private int rear = 0; // index in requestTimes indicating rear of queue
    
    private AtomicLong numRequests = new AtomicLong();  // total number of requests
    private AtomicLong totalRequestTime = new AtomicLong();  // total time spent processing requests
    private String requestType;
    
    RequestStats(int historySize, String type) {
        requestTimes = new long[historySize];
        requestType = type;
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
     * Return string indicating type of request corresponding to statistics
     * 
     * @return string indicating type of request corresponding to statistics
     * 
     * */
    public String getRequestType() {
        return requestType;
    }
    
    /**
     * Return array of most recent request overheads ordered from least to most recent
     * 
     * @return array of most recent request overheads
     * 
     * */
    public long[] getRecentRequestTimes() {
        return requestTimes;
    }

    void recordRequest(long timeInterval) {
        numRequests.incrementAndGet();
        totalRequestTime.addAndGet(timeInterval);
        storeTime(timeInterval);
    }
    
    private synchronized void storeTime(long timeInterval) {
        requestTimes[rear] = timeInterval;
        rear = (rear + 1) % requestTimes.length;
    }

    /**
     * Return string describing summary statistics
     * 
     * @return string describing summary statistics
     * 
     * */
    public String summaryStats() {
        if (numRequests.get() == 0) {
            return "No requests of type " + requestType + "\n";
        }
        String returnVal = "Statistics for requests of type " + requestType + "\n";
        returnVal += "Total requests: " + numRequests.get() + "\n";
        returnVal += "Total request time in milliseconds: " + totalRequestTime.get() + "\n";
        returnVal += "Average milliseconds per request: " +
               (((float) totalRequestTime.get())/numRequests.get()) + "\n";
        return returnVal;
    }

    /**
     * Return string describing recent request overheads
     * 
     * @return string describing recent request overheads
     * 
     * */
    public String recentOverheads() {
        if (numRequests.get() == 0) {
            return "No requests of type " + requestType + "\n";
        }
        int front;
        String returnVal = "Time in milliseconds for recent requests of type "
                + requestType + " ordered from earliest to latest\n";
        if (numRequests.get() <= requestTimes.length) {
            front = 0;
        }
        else {
            front = rear;
        }
        int numOverheads = 0;
        long totalOverhead = 0;
        while (true) {
            returnVal += requestTimes[front] + "\n";
            numOverheads ++;
            totalOverhead += requestTimes[front];
            front = (front + 1) % requestTimes.length;
            if (front == rear) {
                break;
            }
        }
        returnVal += "Total recent requests: " + numOverheads + "\n";
        returnVal += "Total time for recent requests in milliseconds: " + totalOverhead + "\n";
        returnVal += "Average milliseconds per recent request: " +
            (((float) totalOverhead)/numOverheads) + "\n";
        return returnVal;
    }
    
    /**
     * Return string describing all statistics
     * 
     * @return string describing all statistics
     * 
     * */
    public String allStats() {
        return summaryStats() + recentOverheads();
    }

}
