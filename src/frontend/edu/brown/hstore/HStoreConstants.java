package edu.brown.hstore;

import org.voltdb.VoltTable;

public abstract class HStoreConstants {

    // ----------------------------------------------------------------------------
    // STANDARD LOG MESSAGES
    // ----------------------------------------------------------------------------
    
    /**
     * When an HStoreSite is ready to start processing transactions, it will print
     * this message. The BenchmarkController will be waiting for this output.
     */
    public static final String SITE_READY_MSG = "Site is ready for action";
    
    /**
     * This message will get printed when the first non-sysproc transaction request
     * arrives at an HStoreSite. Makes it easier to search through the logs.
     */
    public static final String SITE_FIRST_TXN = "First non-sysproc transaction request recieved";

    // ----------------------------------------------------------------------------
    // NETWORK STUFF
    // ----------------------------------------------------------------------------
    
    public static final int DEFAULT_PORT = 21212;
    
    public static final int MESSENGER_PORT_OFFSET = 10000;
    
    // ----------------------------------------------------------------------------
    // (kowshik)
    //
    // REPLICATION STUFF
    // ----------------------------------------------------------------------------
    
    // (kowshik) experimenting with replication factor of 1 to start with
    public static final int REPLICATION_FACTOR = 2;
    
    // (kowshik) to be safe, setting separate replication proc port away from default port (see above)
    public static final int DEFAULT_REPLICATION_PORT = 41212;
    
    // (kowshik) to be safe, setting high offset for replication msgr port from default replication port (see above)
    public static final int REPLICATION_MSGR_PORT_OFFSET = 10000;
    
    // (kowshik) Starting id for replica. This needs to be handled in a cleaner way in the future.
    public static final int REPLICA_FIRST_ID = 1000;
    
    // (kowshik) Used to denote a non-existent replication id
    public static final int NO_REPLICATION_ID = -1;
    
    // ----------------------------------------------------------------------------
    // EXECUTION STUFF
    // ----------------------------------------------------------------------------
    
    /**
     * Just an empty VoltTable array that we can reuse all around the system
     */
    public static final VoltTable EMPTY_RESULT[] = new VoltTable[0];

    /**
     * Represents a null dependency id
     */
    public static final int NULL_DEPENDENCY_ID = -1;

    /**
     * Default token used to indicate that a txn is not using undo buffers
     * when executing PlanFragments in the EE
     */
    public static final long DISABLE_UNDO_LOGGING_TOKEN = Long.MAX_VALUE;

    /**
     * H-Store's ant build.xml will add this prefix in front of all the configuration
     * parameters listed in the benchmark-specific properties files
     */
    public static final String BENCHMARK_PARAM_PREFIX = "benchmark.";

}
