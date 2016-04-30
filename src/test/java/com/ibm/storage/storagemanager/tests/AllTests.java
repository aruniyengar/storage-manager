package com.ibm.storage.storagemanager.tests;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({  CloudantTests.class, FileTests.class, GuavaTests.class, ObjectStorageTests.class, RedisTests.class,
    SQLTests.class, CompressionTests.class, EncryptionTests.class, MonitoredTests.class })
public class AllTests {

}
