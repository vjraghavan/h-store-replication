/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package edu.brown.hstore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.ClientResponseImpl;
import org.voltdb.DependencySet;
import org.voltdb.HsqlBackend;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.WorkloadTrace;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.ConstraintFailureException;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.MispredictionException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineIPC;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.jni.MockExecutionEngine;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FinishTaskMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.TransactionInfoBaseMessage;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.EstTime;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.DataFragment;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionWorkRequest;
import edu.brown.hstore.Hstoreservice.TransactionWorkResponse;
import edu.brown.hstore.Hstoreservice.TransactionWorkResponse.WorkResult;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.callbacks.TransactionFinishCallback;
import edu.brown.hstore.callbacks.TransactionPrepareCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.AbstractTransaction;
import edu.brown.hstore.dtxn.ExecutionState;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.hstore.dtxn.MapReduceTransaction;
import edu.brown.hstore.dtxn.RemoteTransaction;
import edu.brown.hstore.interfaces.Loggable;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.hstore.util.ThrottlingQueue;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovEstimate;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.brown.utils.TypedPoolableObjectFactory;

/**
 * The main executor of transactional work in the system. Controls running
 * stored procedures and manages the execution engine's running of plan
 * fragments. Interacts with the DTXN system to get work to do. The thread might
 * do other things, but this is where the good stuff happens.
 */
public class PartitionExecutor implements Runnable, Shutdownable, Loggable {
    public static final Logger LOG = Logger.getLogger(PartitionExecutor.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    private static boolean d;
    private static boolean t;
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
        d = debug.get();
        t = trace.get();
    }

    // ----------------------------------------------------------------------------
    // INTERNAL EXECUTION STATE
    // ----------------------------------------------------------------------------

    protected enum ExecutionMode {
        /** Disable processing all transactions until... **/
        DISABLED,
        /** No speculative execution. All transactions are committed immediately **/
        COMMIT_ALL,
        /** Allow read-only txns to return results **/
        COMMIT_READONLY,
        /** All txn responses must wait until the multi-p txn is commited **/ 
        COMMIT_NONE,
    };
    
    // ----------------------------------------------------------------------------
    // GLOBAL CONSTANTS
    // ----------------------------------------------------------------------------
    
    /**
     * Create a new instance of the corresponding VoltProcedure for the given Procedure catalog object
     */
    public class VoltProcedureFactory extends TypedPoolableObjectFactory<VoltProcedure> {
        private final Procedure catalog_proc;
        private final boolean has_java;
        private final Class<? extends VoltProcedure> proc_class;
        
        @SuppressWarnings("unchecked")
        public VoltProcedureFactory(Procedure catalog_proc) {
            super(hstore_conf.site.pool_profiling);
            this.catalog_proc = catalog_proc;
            this.has_java = this.catalog_proc.getHasjava();
            
            // Only try to load the Java class file for the SP if it has one
            Class<? extends VoltProcedure> p_class = null;
            if (catalog_proc.getHasjava()) {
                final String className = catalog_proc.getClassname();
                try {
                    p_class = (Class<? extends VoltProcedure>)Class.forName(className);
                } catch (final ClassNotFoundException e) {
                    LOG.fatal("Failed to load procedure class '" + className + "'", e);
                    System.exit(1);
                }
            }
            this.proc_class = p_class;

        }
        @Override
        public VoltProcedure makeObjectImpl() throws Exception {
            VoltProcedure volt_proc = null;
            try {
                if (this.has_java) {
                    volt_proc = (VoltProcedure)this.proc_class.newInstance();
                } else {
                    volt_proc = new VoltProcedure.StmtProcedure();
                }
                volt_proc.globalInit(PartitionExecutor.this,
                               this.catalog_proc,
                               PartitionExecutor.this.backend_target,
                               PartitionExecutor.this.hsql,
                               PartitionExecutor.this.p_estimator);
            } catch (Exception e) {
                if (d) LOG.warn("Failed to created VoltProcedure instance for " + catalog_proc.getName() , e);
                throw e;
            }
            return (volt_proc);
        }
    };

    /**
     * Procedure Name -> VoltProcedure
     */
    private final Map<String, VoltProcedure> procedures = new HashMap<String, VoltProcedure>();
    
    /**
     * Mapping from SQLStmt batch hash codes (computed by VoltProcedure.getBatchHashCode()) to BatchPlanners
     * The idea is that we can quickly derived the partitions for each unique set of SQLStmt list
     */
    public final Map<Integer, BatchPlanner> POOL_BATCH_PLANNERS = new HashMap<Integer, BatchPlanner>(100);
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------

    private Thread self;
    
    protected int siteId;
    protected int partitionId;
    protected Collection<Integer> localPartitionIds;
    
    /**
     * This is the execution state for the current transaction.
     * There is only one of these per partition, so it must be cleared out for each new txn
     */
    private final ExecutionState execState;

    /**
     * If this flag is enabled, then we need to shut ourselves down and stop running txns
     */
    private Shutdownable.ShutdownState shutdown_state = Shutdownable.ShutdownState.INITIALIZED;
    private Semaphore shutdown_latch;
    
    /**
     * Catalog objects
     */
    protected Catalog catalog;
    protected Cluster cluster;
    protected Database database;
    protected Site site;
    protected Partition partition;

    private final BackendTarget backend_target;
    private final ExecutionEngine ee;
    private final HsqlBackend hsql;
    public static final DBBPool buffer_pool = new DBBPool(true, false);

    /**
     * Runtime Estimators
     */
    protected final PartitionEstimator p_estimator;
    protected final TransactionEstimator t_estimator;
    protected EstimationThresholds thresholds;
    
    protected WorkloadTrace workload_trace;
    
    // ----------------------------------------------------------------------------
    // H-Store Transaction Stuff
    // ----------------------------------------------------------------------------

    protected HStoreSite hstore_site;
    protected HStoreCoordinator hstore_coordinator;
    protected HStoreConf hstore_conf;
    
    // ----------------------------------------------------------------------------
    // Shared VoltProcedure Data Members
    // ----------------------------------------------------------------------------
    
    /**
     * 
     */
    private final ParameterSet[][] voltProc_params; 
    
    // ----------------------------------------------------------------------------
    // Execution State
    // ----------------------------------------------------------------------------
    
    /**
     * We can only have one active distributed transactions at a time.  
     * The multi-partition TransactionState that is currently executing at this partition
     * When we get the response for these txn, we know we can commit/abort the speculatively executed transactions
     */
    private AbstractTransaction current_dtxn = null;
    
    /**
     * Sets of InitiateTaskMessages that are blocked waiting for the outstanding dtxn to commit
     */
    private Set<TransactionInfoBaseMessage> current_blockedTxns = new HashSet<TransactionInfoBaseMessage>();

    private ExecutionMode current_execMode = ExecutionMode.COMMIT_ALL;
    
    private Long currentTxnId = null;

    private final ReentrantLock exec_lock = new ReentrantLock();
    
    /**
     * ClientResponses from speculatively executed transactions that are waiting to be committed 
     */
    private final LinkedBlockingDeque<LocalTransaction> queued_responses = new LinkedBlockingDeque<LocalTransaction>();

    
    /** The time in ms since epoch of the last call to ExecutionEngine.tick(...) */
    private long lastTickTime = 0;
    /** The last txn id that we executed (either local or remote) */
    private volatile Long lastExecutedTxnId = null;
    /** The last txn id that we committed */
    private volatile long lastCommittedTxnId = -1;
    /** The last undoToken that we handed out */
    private long lastUndoToken = 0l;
    
    /**
     * This is the queue of the list of things that we need to execute.
     * The entries may be either InitiateTaskMessages (i.e., start a stored procedure) or
     * FragmentTaskMessage (i.e., execute some fragments on behalf of another transaction)
     */
    private final PriorityBlockingQueue<TransactionInfoBaseMessage> work_queue = new PriorityBlockingQueue<TransactionInfoBaseMessage>(10000, work_comparator) {
        private static final long serialVersionUID = 1L;
        private final List<TransactionInfoBaseMessage> swap = new ArrayList<TransactionInfoBaseMessage>();
        
        @Override
        public int drainTo(Collection<? super TransactionInfoBaseMessage> c) {
            assert(c != null);
            TransactionInfoBaseMessage msg = null;
            int ctr = 0;
            this.swap.clear();
            while ((msg = this.poll()) != null) {
                // All new transaction requests must be put in the new collection
                if (msg instanceof InitiateTaskMessage) {
                    c.add(msg);
                    ctr++;
                // Everything else will get added back in afterwards 
                } else {
                    this.swap.add(msg);
                }
            } // WHILE
            if (this.swap.isEmpty() == false) this.addAll(this.swap);
            return (ctr);
        }
    };
    private final ThrottlingQueue<TransactionInfoBaseMessage> work_throttler;
    
    private static final Comparator<TransactionInfoBaseMessage> work_comparator = new Comparator<TransactionInfoBaseMessage>() {
        @Override
        public int compare(TransactionInfoBaseMessage msg0, TransactionInfoBaseMessage msg1) {
            assert(msg0 != null);
            assert(msg1 != null);
            
            Class<? extends TransactionInfoBaseMessage> class0 = msg0.getClass();
            Class<? extends TransactionInfoBaseMessage> class1 = msg1.getClass();
            
            if (class0.equals(class1)) return (msg0.getTxnId().compareTo(msg1.getTxnId()));

            boolean isFinish0 = class0.equals(FinishTaskMessage.class);
            boolean isFinish1 = class1.equals(FinishTaskMessage.class);
            if (isFinish0 && !isFinish1) return (-1);
            else if (!isFinish0 && isFinish1) return (1);
            
            boolean isWork0 = class0.equals(FragmentTaskMessage.class);
            boolean isWork1 = class1.equals(FragmentTaskMessage.class);
            if (isWork0 && !isWork1) return (-1);
            else if (!isWork0 && isWork1) return (1);
            
            assert(false) : String.format("%s <-> %s", class0, class1);
            return 0;
        }
    };

    // ----------------------------------------------------------------------------
    // TEMPORARY DATA COLLECTIONS
    // ----------------------------------------------------------------------------
    
    /**
     * WorkFragments that we need to send to a remote HStoreSite for execution
     */
    private final List<WorkFragment> tmp_remoteFragmentList = new ArrayList<WorkFragment>();
    /**
     * WorkFragments that we need to send to our own PartitionExecutor
     */
    private final List<WorkFragment> tmp_localWorkFragmentList = new ArrayList<WorkFragment>();
    /**
     * WorkFragments that we need to send to a different PartitionExecutor that is on this same HStoreSite
     */
    private final List<WorkFragment> tmp_localSiteFragmentList = new ArrayList<WorkFragment>();
    
    /**
     * Temporary space used when calling removeInternalDependencies()
     */
    private final HashMap<Integer, List<VoltTable>> tmp_removeDependenciesMap = new HashMap<Integer, List<VoltTable>>();

    /**
     * Remote SiteId -> TransactionWorkRequest.Builder
     */
    private final Map<Integer, TransactionWorkRequest.Builder> tmp_transactionRequestBuildersMap = new HashMap<Integer, TransactionWorkRequest.Builder>();
    
    private final Map<Integer, Set<Integer>> tmp_transactionRequestBuildersParameters = new HashMap<Integer, Set<Integer>>();
    
    private final Map<Integer, Set<Integer>> tmp_transactionRequestBuilderInputs = new HashMap<Integer, Set<Integer>>();
    
    /**
     * PartitionId -> List<VoltTable>
     */
    private final Map<Integer, List<VoltTable>> tmp_EEdependencies = new HashMap<Integer, List<VoltTable>>();
    
    /**
     * List of serialized ParameterSets
     */
    private final List<ByteString> tmp_serializedParams = new ArrayList<ByteString>();
    
    
    /**
     * Reusable ParameterSet arrays
     * Size of ParameterSet[] -> ParameterSet[]
     */
    private final Map<Integer, ParameterSet[]> tmp_parameterSets = new HashMap<Integer, ParameterSet[]>();
    
    // ----------------------------------------------------------------------------
    // PROFILING OBJECTS
    // ----------------------------------------------------------------------------
    
    /**
     * How much time the PartitionExecutor was idle waiting for work to do in its queue
     */
    private final ProfileMeasurement work_idle_time = new ProfileMeasurement("EE_IDLE");
    /**
     * How much time it takes for this PartitionExecutor to execute a transaction
     */
    private final ProfileMeasurement work_exec_time = new ProfileMeasurement("EE_EXEC");
    
    // ----------------------------------------------------------------------------
    // CALLBACKS
    // ----------------------------------------------------------------------------

    /**
     * This will be invoked for each TransactionWorkResponse that comes back from
     * the remote HStoreSites. Note that we don't need to do any counting as to whether
     * a transaction has gotten back all of the responses that it expected. That logic is down
     * below in waitForResponses()
     */
    private final RpcCallback<TransactionWorkResponse> request_work_callback = new RpcCallback<TransactionWorkResponse>() {
        @Override
        public void run(TransactionWorkResponse msg) {
            Long txn_id = msg.getTransactionId();
            AbstractTransaction ts = hstore_site.getTransaction(txn_id);
            assert(ts != null) : "No transaction state exists for txn #" + txn_id;
            
            if (d) LOG.debug(String.format("Processing TransactionWorkResponse for %s with %d results",
                                        ts, msg.getResultsCount()));
            for (int i = 0, cnt = msg.getResultsCount(); i < cnt; i++) {
                WorkResult result = msg.getResults(i); 
                if (t) LOG.trace(String.format("Got %s from partition %d for %s",
                                               result.getClass().getSimpleName(), result.getPartitionId(), ts));
                PartitionExecutor.this.processWorkResult((LocalTransaction)ts, result);
            } // FOR
        }
    }; // END CLASS

    // ----------------------------------------------------------------------------
    // SYSPROC STUFF
    // ----------------------------------------------------------------------------
    
    // Associate the system procedure planfragment ids to wrappers.
    // Planfragments are registered when the procedure wrapper is init()'d.
    private final HashMap<Long, VoltSystemProcedure> m_registeredSysProcPlanFragments = new HashMap<Long, VoltSystemProcedure>();

    public void registerPlanFragment(final long pfId, final VoltSystemProcedure proc) {
        synchronized (m_registeredSysProcPlanFragments) {
            if (!m_registeredSysProcPlanFragments.containsKey(pfId)) {
                assert(m_registeredSysProcPlanFragments.containsKey(pfId) == false) : "Trying to register the same sysproc more than once: " + pfId;
                m_registeredSysProcPlanFragments.put(pfId, proc);
                LOG.trace("Registered @" + proc.getClass().getSimpleName() + " sysproc handle for FragmentId #" + pfId);
            }
        } // SYNCH
    }

    /**
     * SystemProcedures are "friends" with PartitionExecutors and granted
     * access to internal state via m_systemProcedureContext.
     * access to internal state via m_systemProcedureContext.
     */
    public interface SystemProcedureExecutionContext {
        public Catalog getCatalog();
        public Database getDatabase();
        public Cluster getCluster();
        public Site getSite();
        public ExecutionEngine getExecutionEngine();
        public long getLastCommittedTxnId();
        public long getNextUndo();
//        public long getTxnId();
//        public Object getOperStatus();
    }

    protected class SystemProcedureContext implements SystemProcedureExecutionContext {
        public Catalog getCatalog()                 { return catalog; }
        public Database getDatabase()               { return cluster.getDatabases().get("database"); }
        public Cluster getCluster()                 { return cluster; }
        public Site getSite()                       { return site; }
        public ExecutionEngine getExecutionEngine() { return ee; }
        public long getLastCommittedTxnId()         { return PartitionExecutor.this.getLastCommittedTxnId(); }
        public long getNextUndo()                   { return getNextUndoToken(); }
//        public long getTxnId()                      { return getCurrentTxnId(); }
//        public String getOperStatus()               { return VoltDB.getOperStatus(); }
    }

    private final SystemProcedureContext m_systemProcedureContext = new SystemProcedureContext();

    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------

    /**
     * Dummy constructor...
     */
    protected PartitionExecutor() {
        this.work_throttler = null;
        this.ee = null;
        this.hsql = null;
        this.p_estimator = null;
        this.t_estimator = null;
        this.thresholds = null;
        this.catalog = null;
        this.cluster = null;
        this.site = null;
        this.database = null;
        this.backend_target = BackendTarget.HSQLDB_BACKEND;
        this.siteId = 0;
        this.partitionId = 0;
        this.localPartitionIds = null;
        this.execState = null;
        this.voltProc_params = null;
    }

    /**
     * Initialize the StoredProcedure runner and EE for this Site.
     * @param partitionId
     * @param t_estimator
     * @param coordinator
     * @param siteManager
     * @param serializedCatalog A list of catalog commands, separated by
     * newlines that, when executed, reconstruct the complete m_catalog.
     */
    public PartitionExecutor(final int partitionId, final Catalog catalog, final BackendTarget target, PartitionEstimator p_estimator, TransactionEstimator t_estimator) {
        this.hstore_conf = HStoreConf.singleton();
        
        this.work_throttler = new ThrottlingQueue<TransactionInfoBaseMessage>(
                this.work_queue,
                hstore_conf.site.queue_incoming_max_per_partition,
                hstore_conf.site.queue_incoming_release_factor,
                hstore_conf.site.queue_incoming_increase,
                hstore_conf.site.queue_incoming_increase_max
        );
        
        this.catalog = catalog;
        this.partition = CatalogUtil.getPartitionById(this.catalog, partitionId);
        assert(this.partition != null) : "Invalid Partition #" + partitionId;
        this.partitionId = this.partition.getId();
        this.site = this.partition.getParent();
        assert(site != null) : "Unable to get Site for Partition #" + partitionId;
        this.siteId = this.site.getId();
        
        this.execState = new ExecutionState(this);
        
        this.backend_target = target;
        this.cluster = CatalogUtil.getCluster(catalog);
        this.database = CatalogUtil.getDatabase(cluster);

        // The PartitionEstimator is what we use to figure our where our transactions are going to go
        this.p_estimator = p_estimator; // t_estimator.getPartitionEstimator();
        
        // The TransactionEstimator is the runtime piece that we use to keep track of where the 
        // transaction is in its execution workflow. This allows us to make predictions about
        // what kind of things we expect the xact to do in the future
        if (t_estimator == null) { // HACK
            this.t_estimator = new TransactionEstimator(partitionId, p_estimator);    
        } else {
            this.t_estimator = t_estimator; 
        }
        
        // Don't bother with creating the EE if we're on the coordinator
        if (true) { //  || !this.coordinator) {
            // An execution site can be backed by HSQLDB, by volt's EE accessed
            // via JNI or by volt's EE accessed via IPC.  When backed by HSQLDB,
            // the VoltProcedure interface invokes HSQLDB directly through its
            // hsql Backend member variable.  The real volt backend is encapsulated
            // by the ExecutionEngine class. This class has implementations for both
            // JNI and IPC - and selects the desired implementation based on the
            // value of this.eeBackend.
        HsqlBackend hsqlTemp = null;
        ExecutionEngine eeTemp = null;
        try {
            if (d) LOG.debug("Creating EE wrapper with target type '" + target + "'");
            if (this.backend_target == BackendTarget.HSQLDB_BACKEND) {
                hsqlTemp = new HsqlBackend(partitionId);
                final String hexDDL = database.getSchema();
                final String ddl = Encoder.hexDecodeToString(hexDDL);
                final String[] commands = ddl.split(";");
                for (String command : commands) {
                    if (command.length() == 0) {
                        continue;
                    }
                    hsqlTemp.runDDL(command);
                }
                eeTemp = new MockExecutionEngine();
            }
            else if (target == BackendTarget.NATIVE_EE_JNI) {
                org.voltdb.EELibraryLoader.loadExecutionEngineLibrary(true);
                // set up the EE
                eeTemp = new ExecutionEngineJNI(this, cluster.getRelativeIndex(), this.getSiteId(), this.getPartitionId(), this.getHostId(), "localhost");
                eeTemp.loadCatalog(catalog.serialize());
                lastTickTime = System.currentTimeMillis();
                eeTemp.tick( lastTickTime, 0);
            }
            else {
                // set up the EE over IPC
                eeTemp = new ExecutionEngineIPC(this, cluster.getRelativeIndex(), this.getSiteId(), this.getPartitionId(), this.getHostId(), "localhost", target);
                eeTemp.loadCatalog(catalog.serialize());
                lastTickTime = System.currentTimeMillis();
                eeTemp.tick( lastTickTime, 0);
            }
        }
        // just print error info an bail if we run into an error here
        catch (final Exception ex) {
            LOG.fatal("Failed to initialize PartitionExecutor", ex);
            VoltDB.crashVoltDB();
        }
        this.ee = eeTemp;
        this.hsql = hsqlTemp;
        assert(this.ee != null);
        assert(!(this.ee == null && this.hsql == null)) : "Both execution engine objects are empty. This should never happen";
//        } else {
//            this.hsql = null;
//            this.ee = null;
        }

        // Initialize temporary data structures
        for (Site catalog_site : CatalogUtil.getAllSites(this.catalog)) {
            if (catalog_site.getId() != this.siteId) {
                tmp_transactionRequestBuildersParameters.put(catalog_site.getId(), new HashSet<Integer>());
            }
            tmp_transactionRequestBuilderInputs.put(catalog_site.getId(), new HashSet<Integer>());
        } // FOR
        
        // Shared VoltProcedure Cached Objects
        this.voltProc_params = new ParameterSet[hstore_conf.site.planner_max_batch_size][];
        for (int i = 0; i < this.voltProc_params.length; i++) {
            this.voltProc_params[i] = new ParameterSet[i];
            for (int j = 0; j < i; j++) {
                this.voltProc_params[i][j] = new ParameterSet(true);
            } // FOR
        } // FOR
    }
    
    @SuppressWarnings("unchecked")
    protected void initializeVoltProcedures() {
        // load up all the stored procedures
        for (final Procedure catalog_proc : database.getProcedures()) {
            VoltProcedure volt_proc = null;
            
            if (catalog_proc.getHasjava()) {
                // Only try to load the Java class file for the SP if it has one
                Class<? extends VoltProcedure> p_class = null;
                final String className = catalog_proc.getClassname();
                try {
                    p_class = (Class<? extends VoltProcedure>)Class.forName(className);
                    volt_proc = (VoltProcedure)p_class.newInstance();
                } catch (final InstantiationException e) {
                    LOG.fatal("Failed to created VoltProcedure instance for " + catalog_proc.getName() , e);
                    System.exit(1);
                } catch (final IllegalAccessException e) {
                    LOG.fatal("Failed to created VoltProcedure instance for " + catalog_proc.getName() , e);
                    System.exit(1);
                } catch (final ClassNotFoundException e) {
                    LOG.fatal("Failed to load procedure class '" + className + "'", e);
                    System.exit(1);
                }
                
            } else {
                volt_proc = new VoltProcedure.StmtProcedure();
            }
            volt_proc.globalInit(PartitionExecutor.this,
                                 catalog_proc,
                                 this.backend_target,
                                 this.hsql,
                                 this.p_estimator);
            this.procedures.put(catalog_proc.getName(), volt_proc);
        } // FOR
    }

    /**
     * Link this PartitionExecutor with its parent HStoreSite
     * This will initialize the references the various components shared among the PartitionExecutors 
     * @param hstore_site
     */
    public void initHStoreSite(HStoreSite hstore_site) {
        if (t) LOG.trace(String.format("Initializing HStoreSite components at partition %d", this.partitionId));
        assert(this.hstore_site == null);
        this.hstore_site = hstore_site;
        this.hstore_coordinator = hstore_site.getCoordinator();
        this.thresholds = (hstore_site != null ? hstore_site.getThresholds() : null);
        this.localPartitionIds = hstore_site.getLocalPartitionIds();
        
        if (hstore_conf.site.exec_profiling) {
            EventObservable<AbstractTransaction> eo = this.hstore_site.getStartWorkloadObservable();
            this.work_idle_time.resetOnEvent(eo);
            this.work_exec_time.resetOnEvent(eo);
        }
        
        this.initializeVoltProcedures();
    }
    
    // ----------------------------------------------------------------------------
    // MAIN EXECUTION LOOP
    // ----------------------------------------------------------------------------
    
    /**
     * Primary run method that is invoked a single time when the thread is started.
     * Has the opportunity to do startup config.
     */
    @Override
    public void run() {
        assert(this.hstore_site != null);
        assert(this.hstore_coordinator != null);
        assert(this.self == null);
        this.self = Thread.currentThread();
        this.self.setName(HStoreSite.getThreadName(this.hstore_site, this.partitionId));
        
        if (hstore_conf.site.cpu_affinity) {
            this.hstore_site.getThreadManager().registerEEThread(partition);
        }
        
        // *********************************** DEBUG ***********************************
        if (hstore_conf.site.exec_validate_work) {
            LOG.warn("Enabled Distributed Transaction Checking");
        }
        // *********************************** DEBUG ***********************************
        
        // Things that we will need in the loop below
        AbstractTransaction current_txn = null;
        TransactionInfoBaseMessage work = null;
        boolean stop = false;
        Long txn_id = null;
        
        try {
            // Setup shutdown lock
            this.shutdown_latch = new Semaphore(0);
            
            if (d) LOG.debug("Starting PartitionExecutor run loop...");
            while (stop == false && this.isShuttingDown() == false) {
                txn_id = null;
                work = null;
                
                // -------------------------------
                // Poll Work Queue
                // -------------------------------
                try {
                    work = this.work_queue.poll();
                    if (work == null) {
                        if (t) LOG.trace("Partition " + this.partitionId + " queue is empty. Waiting...");
                        if (hstore_conf.site.exec_profiling) this.work_idle_time.start();
                        work = this.work_queue.take();
                        if (hstore_conf.site.exec_profiling) this.work_idle_time.stop();
                    }
                } catch (InterruptedException ex) {
                    if (d && this.isShuttingDown() == false) LOG.debug("Unexpected interuption while polling work queue. Halting PartitionExecutor...", ex);
                    stop = true;
                    break;
                }
                
                txn_id = work.getTxnId();
                current_txn = hstore_site.getTransaction(txn_id);
                if (current_txn == null) {
                    String msg = "No transaction state for txn #" + txn_id;
                    LOG.error(msg + "\n" + work.toString());
                    throw new RuntimeException(msg);
                }
                if (hstore_conf.site.exec_profiling) {
                    this.currentTxnId = txn_id;
                }
                
                // -------------------------------
                // Execute Query Plan Fragments
                // -------------------------------
                if (work instanceof FragmentTaskMessage) {
                    FragmentTaskMessage ftask = (FragmentTaskMessage)work;
                    WorkFragment fragment = ftask.getWorkFragment();
                    assert(fragment != null);
                    ParameterSet parameters[] = current_txn.getAttachedParameterSets();
                    assert(parameters != null);
                    parameters = this.getFragmentParameters(current_txn, fragment, parameters);
                    assert(parameters != null);
                    
                    // At this point we know that we are either the current dtxn or the current dtxn is null
                    // We will allow any read-only transaction to commit if
                    // (1) The WorkFragment for the remote txn is read-only
                    // (2) This txn has always been read-only up to this point at this partition
                    ExecutionMode nextMode = (hstore_conf.site.exec_speculative_execution == false ? ExecutionMode.DISABLED :
                                              fragment.getReadOnly() && current_txn.isExecReadOnly(this.partitionId) ?
                                                      ExecutionMode.COMMIT_READONLY : ExecutionMode.COMMIT_NONE);
                    exec_lock.lock();
                    try {
                        // There is no current DTXN, so that means its us!
                        if (this.current_dtxn == null) {
                            this.setCurrentDtxn(current_txn);
                            if (d) LOG.debug(String.format("Marking %s as current DTXN on partition %d [nextMode=%s]",
                                                                    current_txn, this.partitionId, nextMode));                    
                        }
                        this.setExecutionMode(current_txn, nextMode);
                    } finally {
                        exec_lock.unlock();
                    } // SYNCH
                    
                    this.processWorkFragment(current_txn, fragment, parameters);

                // -------------------------------
                // Invoke Stored Procedure
                // -------------------------------
                } else if (work instanceof InitiateTaskMessage) {
                    if (hstore_conf.site.exec_profiling) this.work_exec_time.start();
                    InitiateTaskMessage itask = (InitiateTaskMessage)work;
                    
                    // If this is a MapReduceTransaction handle, we actually want to get the 
                    // inner LocalTransaction handle for this partition. The MapReduceTransaction
                    // is just a placeholder
                    if (current_txn instanceof MapReduceTransaction) {
                        MapReduceTransaction orig_ts = (MapReduceTransaction)current_txn; 
                        current_txn = orig_ts.getLocalTransaction(this.partitionId);
                        assert(current_txn != null) : "Unexpected null LocalTransaction handle from " + orig_ts; 
                    }

                    this.processInitiateTaskMessage((LocalTransaction)current_txn, itask);
                    if (hstore_conf.site.exec_profiling) this.work_exec_time.stop();
                    
                // -------------------------------
                // Finish Transaction
                // -------------------------------
                } else if (work instanceof FinishTaskMessage) {
//                    if (hstore_conf.site.exec_profiling) this.work_exec_time.start();
                    if(d) LOG.debug("<FinishTaskMessage> for txn: " + current_txn);
//                    if (current_txn instanceof MapReduceTransaction) {
//                        MapReduceTransaction orig_ts = (MapReduceTransaction)current_txn; 
//                        current_txn = orig_ts.getLocalTransaction(this.partitionId);
//                        //this.setCurrentDtxn(current_txn);
//                        if(d) LOG.debug("<FinishTaskMessage> I am a MapReduceTransaction: " + current_txn);
//                        assert(current_txn != null) : "Unexpected null LocalTransaction handle from " + orig_ts;
//                    }
                    
                    FinishTaskMessage ftask = (FinishTaskMessage)work;
                    this.finishTransaction(current_txn, (ftask.getStatus() == Hstoreservice.Status.OK));
//                    if (hstore_conf.site.exec_profiling) this.work_exec_time.stop();
                    
                // -------------------------------
                // BAD MOJO!
                // -------------------------------
                } else if (work != null) {
                    throw new RuntimeException("Unexpected work message in queue: " + work);
                }

                // Is there a better way to do this?
                this.work_throttler.checkThrottling(false);
                
                if (hstore_conf.site.exec_profiling && this.currentTxnId != null) {
                    this.lastExecutedTxnId = this.currentTxnId;
                    this.currentTxnId = null;
                }
            } // WHILE
        } catch (final Throwable ex) {
            if (this.isShuttingDown() == false) {
                ex.printStackTrace();
                LOG.fatal(String.format("Unexpected error for PartitionExecutor partition #%d [%s]%s",
                                        this.partitionId, (current_txn != null ? " - " + current_txn : ""), ex), ex);
                if (current_txn != null) LOG.fatal("TransactionState Dump:\n" + current_txn.debug());
                
            }
            this.hstore_coordinator.shutdownCluster(new Exception(ex));
        } finally {
            String txnDebug = "";
            if (d && current_txn != null && current_txn.getBasePartition() == this.partitionId) {
                txnDebug = "\n" + current_txn.debug();
            }
            LOG.warn(String.format("Partition %d PartitionExecutor is stopping.%s%s",
                                   this.partitionId, (txn_id != null ? " In-Flight Txn: #" + txn_id : ""), txnDebug));
            
            // Release the shutdown latch in case anybody waiting for us
            this.shutdown_latch.release();
            
            // Stop HStoreMessenger (because we're nice)
            if (this.isShuttingDown() == false) {
                if (this.hstore_coordinator != null) this.hstore_coordinator.shutdown();
            }
        }
    }

    public void tick() {
        // invoke native ee tick if at least one second has passed
        final long time = EstTime.currentTimeMillis();
        if ((time - lastTickTime) >= 1000) {
            if ((lastTickTime != 0) && (ee != null)) {
                ee.tick(time, lastCommittedTxnId);
            }
            lastTickTime = time;
        }
    }

    @Override
    public void updateLogging() {
        d = debug.get();
        t = trace.get();
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    public ExecutionEngine getExecutionEngine() {
        return (this.ee);
    }
    public Thread getExecutionThread() {
        return (this.self);
    }
    public PartitionEstimator getPartitionEstimator() {
        return (this.p_estimator);
    }
    public TransactionEstimator getTransactionEstimator() {
        return (this.t_estimator);
    }
    public ThrottlingQueue<TransactionInfoBaseMessage> getThrottlingQueue() {
        return (this.work_throttler);
    }
    
    public HStoreSite getHStoreSite() {
        return (this.hstore_site);
    }
    public HStoreConf getHStoreConf() {
        return (this.hstore_conf);
    }
    public HStoreCoordinator getHStoreCoordinator() {
        return (this.hstore_coordinator);
    }

    public Site getCatalogSite() {
        return (this.site);
    }
    public int getHostId() {
        return (this.site.getHost().getRelativeIndex());
    }
    public int getSiteId() {
        return (this.siteId);
    }
    public Partition getPartition() {
        return (this.partition);
    }
    public int getPartitionId() {
        return (this.partitionId);
    }
    public Collection<Integer> getLocalPartitionIds() {
        return (this.localPartitionIds);
    }
    
    public Long getLastExecutedTxnId() {
        return (this.lastExecutedTxnId);
    }
    public Long getLastCommittedTxnId() {
        return (this.lastCommittedTxnId);
    }
    
    /**
     * Return a cached ParameterSet array. This should only be 
     * called by the VoltProcedures managed by this PartitionExecutor.
     * This is just to reduce the number of objects that we need to allocate
     * @param size
     * @return
     */
    public ParameterSet[] getParameterSet(int size) {
        assert(size < this.voltProc_params.length);
        return (this.voltProc_params[size]);
    }

    /**
     * Returns the next undo token to use when hitting up the EE with work
     * MAX_VALUE = no undo
     * @param txn_id
     * @return
     */
    public long getNextUndoToken() {
        return (++this.lastUndoToken);
    }
    
    /**
     * Set the current ExecutionMode for this executor
     * @param mode
     * @param txn_id
     */
    private void setExecutionMode(AbstractTransaction ts, ExecutionMode mode) {
        if (d && this.current_execMode != mode) {
//        if (this.exec_mode != mode) {
            LOG.debug(String.format("Setting ExecutionMode for partition %d to %s because of %s [currentDtxn=%s, origMode=%s]",
                                    this.partitionId, mode, ts, this.current_dtxn, this.current_execMode));
        }
        assert(mode != ExecutionMode.COMMIT_READONLY || (mode == ExecutionMode.COMMIT_READONLY && this.current_dtxn != null)) :
            String.format("%s is trying to set partition %d to %s when the current DTXN is null?", ts, this.partitionId, mode);
        this.current_execMode = mode;
    }
    public ExecutionMode getExecutionMode() {
        return (this.current_execMode);
    }
    public AbstractTransaction getCurrentDtxn() {
        return (this.current_dtxn);
    }
    public Long getCurrentTxnId() {
        return (this.currentTxnId);
    }
    
    public int getBlockedQueueSize() {
        return (this.current_blockedTxns.size());
    }
    public int getWaitingQueueSize() {
        return (this.queued_responses.size());
    }
    public int getWorkQueueSize() {
        return (this.work_queue.size());
    }
    public ProfileMeasurement getWorkIdleTime() {
        return (this.work_idle_time);
    }
    public ProfileMeasurement getWorkExecTime() {
        return (this.work_exec_time);
    }
    /**
     * Returns the number of txns that have been invoked on this partition
     * @return
     */
    public long getTransactionCounter() {
        return (this.work_exec_time.getInvocations());
    }
    
    /**
     * Returns the VoltProcedure instance for a given stored procedure name
     * @param proc_name
     * @return
     */
    public VoltProcedure getVoltProcedure(String proc_name) {
        return (this.procedures.get(proc_name));
    }

    private ParameterSet[] getFragmentParameters(AbstractTransaction ts, WorkFragment fragment, ParameterSet allParams[]) {
        int num_fragments = fragment.getFragmentIdCount();
        ParameterSet fragmentParams[] = tmp_parameterSets.get(num_fragments);
        if (fragmentParams == null) {
            fragmentParams = new ParameterSet[num_fragments];
            tmp_parameterSets.put(num_fragments, fragmentParams); 
        }
        assert(fragmentParams != null);
        assert(fragmentParams.length == num_fragments);
        
        for (int i = 0; i < num_fragments; i++) {
            int stmt_index = fragment.getStmtIndex(i);
            assert(stmt_index < allParams.length) :
                String.format("StatementIndex is %d but there are only %d ParameterSets for %s",
                              stmt_index, allParams.length, ts); 
            fragmentParams[i] = allParams[stmt_index];
        } // FOR
        return (fragmentParams);
    }
    
    private Map<Integer, List<VoltTable>> getFragmentInputs(AbstractTransaction ts, WorkFragment fragment, Map<Integer, List<VoltTable>> inputs) {
        Map<Integer, List<VoltTable>> attachedInputs = ts.getAttachedInputDependencies();
        assert(attachedInputs != null);
        boolean is_local = (ts instanceof LocalTransaction);
        
        if (d) LOG.debug(String.format("Attempting to retrieve input dependencies for %s WorkFragment [isLocal=%s]:\n%s",
                         ts, is_local, fragment));
        for (int i = 0, cnt = fragment.getFragmentIdCount(); i < cnt; i++) {
            int stmt_index = fragment.getStmtIndex(i);
            WorkFragment.InputDependency input_dep_ids = fragment.getInputDepId(i);
            for (int input_dep_id : input_dep_ids.getIdsList()) {
                if (input_dep_id == HStoreConstants.NULL_DEPENDENCY_ID) continue;

                // If the Transaction is on the same HStoreSite, then all the 
                // input dependencies will be internal and can be retrieved locally
                if (is_local) {
                    List<VoltTable> deps = ((LocalTransaction)ts).getInternalDependency(stmt_index, input_dep_id);
                    assert(deps != null);
                    assert(inputs.containsKey(input_dep_id) == false);
                    inputs.put(input_dep_id, deps);
                    if (d) LOG.debug(String.format("Retrieved %d INTERNAL VoltTables for <Stmt #%d, DependencyId #%d> in %s",
                                                                          deps.size(), stmt_index, input_dep_id, ts));
                }
                // Otherwise they will be "attached" inputs to the RemoteTransaction handle
                // We should really try to merege these two concepts into a single function call
                else if (attachedInputs.containsKey(input_dep_id)) {
                    List<VoltTable> deps = attachedInputs.get(input_dep_id);
                    List<VoltTable> pDeps = null;
                    // XXX: Do we actually need to copy these???
                    // XXX: I think we only need to copy if we're debugging the tables!
                    if (d) { // this.firstPartition == false) {
                        pDeps = new ArrayList<VoltTable>();
                        for (VoltTable vt : deps) {
                            // TODO: Move into VoltTableUtil
                            ByteBuffer buffer = vt.getTableDataReference();
                            byte arr[] = new byte[vt.getUnderlyingBufferSize()]; // FIXME
                            buffer.get(arr, 0, arr.length);
                            pDeps.add(new VoltTable(ByteBuffer.wrap(arr), true));
                        }
                    } else {
                        pDeps = deps;
                    }
                    inputs.put(input_dep_id, pDeps); 
                    if (d) LOG.debug(String.format("Retrieved %d ATTACHED VoltTables for <Stmt #%d, DependencyId #%d> in %s",
                                                                          deps.size(), stmt_index, input_dep_id, ts));
                }

            } // FOR (inputs)
        } // FOR (fragments)
        if (d) {
            if (inputs.isEmpty() == false) {
                LOG.debug(String.format("Retrieved %d InputDependencies for %s %s on partition %d\n%s",
                                        inputs.size(), ts, fragment.getFragmentIdList(), fragment.getPartitionId(), "XXXX")); // StringUtil.formatMaps(inputs)));
            } else {
                LOG.warn(String.format("No InputDependencies retrieved for %s %s on partition %d",
                                       ts, fragment.getFragmentIdList(), fragment.getPartitionId()));
            }
        }
        return (inputs);
    }
    
    
    /**
     * 
     * @param ts
     */
    private void setCurrentDtxn(AbstractTransaction ts) {
        // There can never be another current dtxn still unfinished at this partition!
        assert(ts == null || this.current_blockedTxns.isEmpty()) :
            String.format("Concurrent multi-partition transactions at partition %d: Orig[%s] <=> New[%s] / BlockedQueue:%d",
                          this.partitionId, this.current_dtxn, ts, this.current_blockedTxns.size());
        assert(ts == null || this.current_dtxn == null || this.current_dtxn.isInitialized() == false) :
            String.format("Concurrent multi-partition transactions at partition %d: Orig[%s] <=> New[%s] / BlockedQueue:%d",
                          this.partitionId, this.current_dtxn, ts, this.current_blockedTxns.size());
        if (d) LOG.debug(String.format("Setting %s as the current DTXN for partition #%d [previous=%s]",
                                       ts, this.partitionId, this.current_dtxn));
        this.current_dtxn = ts;
    }
    
    // ---------------------------------------------------------------
    // PartitionExecutor API
    // ---------------------------------------------------------------
    
    /**
     * New work from the coordinator that this local site needs to execute (non-blocking)
     * This method will simply chuck the task into the work queue.
     * We should not be sent an InitiateTaskMessage here!
     * @param task
     * @param callback the RPC handle to send the response to
     */
    public void queueWork(AbstractTransaction ts, FragmentTaskMessage task) {
        assert(ts.isInitialized());
        
        this.work_queue.add(task);
        if (d) LOG.debug(String.format("Added multi-partition %s for %s to front of partition %d work queue [size=%d]",
                                                task.getClass().getSimpleName(), ts, this.partitionId, this.work_queue.size()));
    }
    
    /**
     * Put the finish request for the transaction into the queue
     * @param task
     * @param callback the RPC handle to send the response to
     */
    public void queueFinish(AbstractTransaction ts, Status status) {
        assert(ts.isInitialized());
        
        FinishTaskMessage task = ts.getFinishTaskMessage(status);
        this.work_queue.add(task);
        if (d) LOG.debug(String.format("Added multi-partition %s for %s to front of partition %d work queue [size=%d]",
                                       task.getClass().getSimpleName(), ts, this.partitionId, this.work_queue.size()));
    }

    /**
     * New work for a local transaction
     * @param ts
     * @param task
     * @param callback
     */
    public boolean queueNewTransaction(LocalTransaction ts) {
        assert(ts != null) : "Unexpected null transaction handle!";
        final InitiateTaskMessage task = ts.getInitiateTaskMessage();
        final boolean singlePartitioned = ts.isPredictSinglePartition();
        boolean success = true;
        
        if (d) LOG.debug(String.format("%s - Queuing new transaction execution request on partition %d [currentDtxn=%s, mode=%s, taskHash=%d]",
                                       ts, this.partitionId, this.current_dtxn, this.current_execMode, task.hashCode()));
        
        // If we're a single-partition and speculative execution is enabled, then we can always set it up now
        if (hstore_conf.site.exec_speculative_execution && singlePartitioned && this.current_execMode != ExecutionMode.DISABLED) {
            if (d) LOG.debug(String.format("%s - Adding to work queue at partition %d [size=%d]", ts, this.partitionId, this.work_queue.size()));
            success = this.work_throttler.offer(task, false);
            
        // Otherwise figure out whether this txn needs to be blocked or not
        } else {
            if (d) LOG.debug(String.format("%s - Attempting to add %s to partition %d queue [currentTxn=%s]",
                                           ts, task.getClass().getSimpleName(), this.partitionId, this.currentTxnId));
            
            exec_lock.lock();
            try {
                // No outstanding DTXN
                if (this.current_dtxn == null && this.current_execMode != ExecutionMode.DISABLED) {
                    if (d) LOG.debug(String.format("%s - Adding %s to work queue [size=%d]",
                                                   ts, task.getClass().getSimpleName(), this.work_queue.size()));
                    // Only use the throttler for single-partition txns
                    if (singlePartitioned) {
                        success = this.work_throttler.offer(task, false);
                    } else {
                        // this.work_queue.addFirst(task);
                        this.work_queue.add(task);
                    }
                }
                // Add the transaction request to the blocked queue
                else {
                    // TODO: This is where we can check whether this new transaction request is commutative 
                    //       with the current dtxn. If it is, then we know that we don't
                    //       need to block it or worry about whether it will conflict with the current dtxn
                    if (d) LOG.debug(String.format("%s - Blocking until dtxn %s finishes", ts, this.current_dtxn));
                    this.current_blockedTxns.add(task);
                }
            } finally {
                exec_lock.unlock();
            } // SYNCH
        }
        
        if (success == false) {
            // Depending on what we need to do for this type txn, we will send
            // either an ABORT_THROTTLED or an ABORT_REJECT in our response
            // An ABORT_THROTTLED means that the client will back-off of a bit
            // before sending another txn request, where as an ABORT_REJECT means
            // that it will just try immediately
            Hstoreservice.Status status = ((singlePartitioned ? hstore_conf.site.queue_incoming_throttle : hstore_conf.site.queue_dtxn_throttle) ? 
                                        Hstoreservice.Status.ABORT_THROTTLED :
                                        Hstoreservice.Status.ABORT_REJECT);
            
            if (d) LOG.debug(String.format("%s - Hit with a %s response from partition %d [currentTxn=%s, throttled=%s, queueSize=%d]",
                                           ts, status, this.partitionId, this.currentTxnId,
                                           this.work_throttler.isThrottled(), this.work_throttler.size()));
            if (singlePartitioned == false) {
                TransactionFinishCallback finish_callback = ts.initTransactionFinishCallback(Hstoreservice.Status.ABORT_THROTTLED);
                hstore_coordinator.transactionFinish(ts, status, finish_callback);
            }
            hstore_site.transactionReject(ts, status);
        }
        return (success);
    }

    // ---------------------------------------------------------------
    // WORK QUEUE PROCESSING METHODS
    // ---------------------------------------------------------------
    

    /**
     * Enable speculative execution mode for this partition
     * The given TransactionId is the transaction that we need to wait to finish before
     * we can release the speculatively executed transactions
     * Returns true if speculative execution was enabled at this partition
     * @param txn_id
     * @param force
     * @return
     */
    public boolean enableSpeculativeExecution(AbstractTransaction ts, boolean force) {
        assert(ts != null) : "Null transaction handle???";
        // assert(this.speculative_execution == SpeculateType.DISABLED) : "Trying to enable spec exec twice because of txn #" + txn_id;
        
        if (d) LOG.debug(String.format("%s - Checking whether txn is read-only at partition %d [readOnly=%s]",
                                       ts, this.partitionId, ts.isExecReadOnly(this.partitionId)));
        
        // Check whether the txn that we're waiting for is read-only.
        // If it is, then that means all read-only transactions can commit right away
        if (ts.isExecReadOnly(this.partitionId)) {
            ExecutionMode newMode = ExecutionMode.COMMIT_READONLY;
            if (d) LOG.debug(String.format("%s - Attempting to enable %s speculative execution at partition %d [currentMode=%s]",
                                           ts, newMode, partitionId, this.current_execMode));
            exec_lock.lock();
            try {
                if (this.current_dtxn == ts && this.current_execMode != ExecutionMode.DISABLED) {
                    this.setExecutionMode(ts, newMode);
                    this.releaseBlockedTransactions(ts, true);
                    if (d) LOG.debug(String.format("%s - Enabled %s speculative execution at partition %d",
                                                   ts, this.current_execMode, partitionId));
                    return (true);
                }
            } finally {
                exec_lock.unlock();
            } // SYNCH
        }
        return (false);
    }
    
    /**
     * Process a FragmentResponseMessage and update the TransactionState accordingly
     * @param ts
     * @param fresponse
     */
    protected void processWorkResult(LocalTransaction ts, WorkResult fresponse) {
        if (d) LOG.debug(String.format("Processing FragmentResponseMessage for %s on partition %d [srcPartition=%d, deps=%d]",
                                       ts, this.partitionId, fresponse.getPartitionId(), fresponse.getOutputCount()));
        
        // If the Fragment failed to execute, then we need to abort the Transaction
        // Note that we have to do this before we add the responses to the TransactionState so that
        // we can be sure that the VoltProcedure knows about the problem when it wakes the stored 
        // procedure back up
        if (fresponse.getStatus() != Hstoreservice.Status.OK) {
            if (t) LOG.trace(String.format("Received non-success response %s from partition %d for %s",
                                                    fresponse.getStatus(), fresponse.getPartitionId(), ts));

            SerializableException error = null;
            if (hstore_conf.site.txn_profiling) ts.profiler.startDeserialization();
            try {
                error = SerializableException.deserializeFromBuffer(fresponse.getError().asReadOnlyByteBuffer());
            } catch (Exception ex) {
                throw new RuntimeException(String.format("Failed to deserialize SerializableException from partition %d for %s [bytes=%d]",
                                           fresponse.getPartitionId(), ts, fresponse.getError().size()), ex);
            } finally {
                if (hstore_conf.site.txn_profiling) ts.profiler.stopDeserialization();
            }
            ts.setPendingError(error);
        }
        
        if (hstore_conf.site.txn_profiling) ts.profiler.startDeserialization();
        for (DataFragment output : fresponse.getOutputList()) {
            if (t) LOG.trace(String.format("Storing intermediate results from partition %d for %s",
                                                    fresponse.getPartitionId(), ts));
            for (ByteString bs : output.getDataList()) {
                VoltTable vt = null;
                if (bs.isEmpty() == false) {
                    FastDeserializer fd = new FastDeserializer(bs.asReadOnlyByteBuffer());
                    try {
                        vt = fd.readObject(VoltTable.class);
                    } catch (Exception ex) {
                        throw new RuntimeException("Failed to deserialize VoltTable from partition " + fresponse.getPartitionId() + " for " + ts, ex);
                    }
                }
                ts.addResult(fresponse.getPartitionId(), output.getId(), vt);
            } // FOR (output)
        } // FOR (dependencies)
        if (hstore_conf.site.txn_profiling) ts.profiler.stopDeserialization();
    }
    
    /**
     * Execute a new transaction based on an InitiateTaskMessage
     * @param itask
     */
    protected void processInitiateTaskMessage(LocalTransaction ts, InitiateTaskMessage itask) throws InterruptedException {
        if (hstore_conf.site.txn_profiling) ts.profiler.startExec();
        
        ExecutionMode before_mode = ExecutionMode.COMMIT_ALL;
        boolean predict_singlePartition = ts.isPredictSinglePartition();
        
        if (t) LOG.trace(String.format("Attempting to begin processing %s for %s on partition %d [taskHash=%d]",
                                       itask.getClass().getSimpleName(), ts, this.partitionId, itask.hashCode()));
        // If this is going to be a multi-partition transaction, then we will mark it as the current dtxn
        // for this PartitionExecutor.
        if (predict_singlePartition == false) {
            this.exec_lock.lock();
            try {
                if (this.current_dtxn != null) {
                    this.current_blockedTxns.add(itask);
                    return;
                }
                this.setCurrentDtxn(ts);
                // 2011-11-14: We don't want to set the execution mode here, because we know that we
                //             can check whether we were read-only after the txn finishes
                if (d) LOG.debug(String.format("Marking %s as current DTXN on Partition %d [isLocal=%s, execMode=%s]",
                                               ts, this.partitionId, true, this.current_execMode));                    
                before_mode = this.current_execMode;
            } finally {
                exec_lock.unlock();
            } // SYNCH
        } else {
            exec_lock.lock();
            try {
                // If this is a single-partition transaction, then we need to check whether we are being executed
                // under speculative execution mode. We have to check this here because it may be the case that we queued a
                // bunch of transactions when speculative execution was enabled, but now the transaction that was ahead of this 
                // one is finished, so now we're just executing them regularly
                if (this.current_execMode != ExecutionMode.COMMIT_ALL) {
                    assert(this.current_dtxn != null) : String.format("Invalid execution mode %s without a dtxn at partition %d", this.current_execMode, this.partitionId);
                    
                    // HACK: If we are currently under DISABLED mode when we get this, then we just need to block the transaction
                    // and return back to the queue. This is easier than having to set all sorts of crazy locks
                    if (this.current_execMode == ExecutionMode.DISABLED) {
                        if (d) LOG.debug(String.format("Blocking single-partition %s until dtxn %s finishes [mode=%s]", ts, this.current_dtxn, this.current_execMode));
                        this.current_blockedTxns.add(itask);
                        return;
                    }
                    
                    before_mode = this.current_execMode;
                    if (hstore_conf.site.exec_speculative_execution) {
                        ts.setSpeculative(true);
                        if (d) LOG.debug(String.format("Marking %s as speculatively executed on partition %d [txnMode=%s, dtxn=%s]", ts, this.partitionId, before_mode, this.current_dtxn));
                    }
                }
            } finally {
                exec_lock.unlock();
            } // SYNCH
        }
        
        // Always reset the ExecutionState
        this.execState.clear();
        ts.setExecutionState(this.execState);
        
        VoltProcedure volt_proc = this.procedures.get(itask.getStoredProcedureName());
        assert(volt_proc != null) : "No VoltProcedure for " + ts;
        
        if (d) {
            LOG.debug(String.format("%s - Starting execution of txn [txnMode=%s, mode=%s]",
                                    ts, before_mode, this.current_execMode));
            if (t) LOG.trace("Current Transaction at partition #" + this.partitionId + "\n" + ts.debug());
        }
            
        ClientResponseImpl cresponse = null;
        try {
            cresponse = (ClientResponseImpl)volt_proc.call(ts,itask.getParameters()); // Blocking...
        // VoltProcedure.call() should handle any exceptions thrown by the transaction
        // If we get anything out here then that's bad news
        } catch (Throwable ex) {
            if (this.isShuttingDown() == false) {
                SQLStmt last[] = volt_proc.voltLastQueriesExecuted();
                System.err.println("ERROR: " + ex);
                LOG.fatal("Unexpected error while executing " + ts, ex);
                if (last.length > 0) {
                    LOG.fatal(String.format("Last Queries Executed [%d]: %s", last.length, Arrays.toString(last)));
                }
                LOG.fatal("LocalTransactionState Dump:\n" + ts.debug());
                this.crash(ex);
            }
        }
        // If this is a MapReduce job, then we can just ignore the ClientResponse
        // and return immediately
        if (ts.isMapReduce()) {
            return;
        } else if (cresponse == null) {
            assert(this.isShuttingDown()) : String.format("No ClientResponse for %s???", ts);
            return;
        }
        
        Hstoreservice.Status status = cresponse.getStatus();
        if (d) LOG.debug(String.format("Finished execution of %s [status=%s, beforeMode=%s, currentMode=%s]",
                                       ts, status, before_mode, this.current_execMode));

        // We assume that most transactions are not speculatively executed and are successful
        // Therefore we don't want to grab the exec_mode lock here.
        if (predict_singlePartition == false || this.canProcessClientResponseNow(ts, status, before_mode)) {
            if (d) LOG.debug(String.format("%s - Sending ClientResponse back directly [status=%s]",
                                           ts, cresponse.getStatus()));
            this.processClientResponse(ts, cresponse);
        } else {
            exec_lock.lock();
            try {
                if (this.canProcessClientResponseNow(ts, status, before_mode)) {
                    if (d) LOG.debug(String.format("%s - Sending ClientResponse back directly [status=%s]",
                                                   ts, cresponse.getStatus()));
                    this.processClientResponse(ts, cresponse);
                // Otherwise always queue our response, since we know that whatever thread is out there
                // is waiting for us to finish before it drains the queued responses
                } else {
                    // If the transaction aborted, then we can't execute any transaction that touch the tables that this guy touches
                    // But since we can't just undo this transaction without undoing everything that came before it, we'll just
                    // disable executing all transactions until the multi-partition transaction commits
                    // NOTE: We don't need acquire the 'exec_mode' lock here, because we know that we either executed in non-spec mode, or 
                    // that there already was a multi-partition transaction hanging around.
                    if (status != Hstoreservice.Status.OK && ts.isExecReadOnlyAllPartitions() == false) {
                        this.setExecutionMode(ts, ExecutionMode.DISABLED);
                        int blocked = this.work_queue.drainTo(this.current_blockedTxns);
                        if (t && blocked > 0)
                            LOG.trace(String.format("Blocking %d transactions at partition %d because ExecutionMode is now %s",
                                                    blocked, this.partitionId, this.current_execMode));
                        if (d) LOG.debug(String.format("Disabling execution on partition %d because speculative %s aborted", this.partitionId, ts));
                    }
                    if (t) LOG.trace(String.format("%s - Queuing ClientResponse [status=%s, origMode=%s, newMode=%s, dtxn=%s]",
                                                   ts, cresponse.getStatus(), before_mode, this.current_execMode, this.current_dtxn));
                    this.queueClientResponse(ts, cresponse);
                }
            } finally {
                exec_lock.unlock();
            } // SYNCH
        }
        
        volt_proc.finish();
    }
    
    /**
     * Determines whether a finished transaction that executed locally can have their ClientResponse processed immediately
     * or if it needs to wait for the response from the outstanding multi-partition transaction for this partition 
     * (1) This is the multi-partition transaction that everyone is waiting for
     * (2) The transaction was not executed under speculative execution mode 
     * (3) The transaction does not need to wait for the multi-partition transaction to finish first
     * @param ts
     * @param status
     * @param before_mode
     * @return
     */
    private boolean canProcessClientResponseNow(LocalTransaction ts, Hstoreservice.Status status, ExecutionMode before_mode) {
        if (d) LOG.debug(String.format("%s - Checking whether to process response now [status=%s, singlePartition=%s, readOnly=%s, beforeMode=%s, currentMode=%s]",
                                       ts, status, ts.isExecSinglePartition(), ts.isExecReadOnly(this.partitionId), before_mode, this.current_execMode));
        // Commit All
        if (this.current_execMode == ExecutionMode.COMMIT_ALL) {
            return (true);
            
        // Process successful txns based on the mode that it was executed under
        } else if (status == Hstoreservice.Status.OK) {
            switch (before_mode) {
                case COMMIT_ALL:
                    return (true);
                case COMMIT_READONLY:
                    return (ts.isExecReadOnly(this.partitionId));
                case COMMIT_NONE: {
                    return (false);
                }
                default:
                    throw new RuntimeException("Unexpectd execution mode: " + before_mode); 
            } // SWITCH
        }
        // Anything mispredicted should be processed right away
        else if (status == Hstoreservice.Status.ABORT_MISPREDICT) {
            return (true);
        }    
        // If the transaction aborted and it was read-only thus far, then we want to process it immediately
        else if (status != Hstoreservice.Status.OK && ts.isExecReadOnly(this.partitionId)) {
            return (true);
        }
        // If this txn threw a user abort, and the current outstanding dtxn is read-only
        // then it's safe for us to rollback
        else if (status == Hstoreservice.Status.ABORT_USER && (this.current_dtxn != null && this.current_dtxn.isExecReadOnly(this.partitionId))) {
            return (true);
        }
        
        assert(this.current_execMode != ExecutionMode.COMMIT_ALL) :
            String.format("Queuing ClientResponse for %s when in non-specutative mode [mode=%s, status=%s]",
                          ts, this.current_execMode, status);
        return (false);
    }
    
    /**
     * 
     * @param wfrag
     * @throws Exception
     */
    private void processWorkFragment(AbstractTransaction ts, WorkFragment wfrag, ParameterSet parameters[]) {
        assert(this.partitionId == wfrag.getPartitionId()) :
            String.format("Tried to execute WorkFragment %s for %s on partition %d but it was suppose to be executed on partition %d",
                          wfrag.getFragmentIdList(), ts, this.partitionId, wfrag.getPartitionId());
        
        // A txn is "local" if the Java is executing at the same site as we are
        boolean is_local = ts.isExecLocal(this.partitionId);
        boolean is_dtxn = (ts instanceof LocalTransaction == false);
        if (d) LOG.debug(String.format("Executing FragmentTaskMessage %s [basePartition=%d, isLocal=%s, isDtxn=%s, fragments=%s]",
                                                ts, ts.getBasePartition(), is_local, is_dtxn, wfrag.getFragmentIdCount()));

        // If this txn isn't local, then we have to update our undoToken
        if (is_local == false) {
            ts.initRound(this.partitionId, this.getNextUndoToken());
            ts.startRound(this.partitionId);
        }
        
        DependencySet result = null;
        Hstoreservice.Status status = Hstoreservice.Status.OK;
        SerializableException error = null;
        
        try {
            result = this.executeWorkFragment(ts, wfrag, parameters);
        } catch (ConstraintFailureException ex) {
            LOG.fatal("Hit an ConstraintFailureException for " + ts, ex);
            status = Hstoreservice.Status.ABORT_UNEXPECTED;
            error = ex;
        } catch (EEException ex) {
            LOG.fatal("Hit an EE Error for " + ts, ex);
            this.crash(ex);
            status = Hstoreservice.Status.ABORT_UNEXPECTED;
            error = ex;
        } catch (SQLException ex) {
            LOG.warn("Hit a SQL Error for " + ts, ex);
            status = Hstoreservice.Status.ABORT_UNEXPECTED;
            error = ex;
        } catch (Throwable ex) {
            LOG.error("Something unexpected and bad happended for " + ts, ex);
            status = Hstoreservice.Status.ABORT_UNEXPECTED;
            error = new SerializableException(ex);
        } finally {
            // Success, but without any results???
            if (result == null && status == Hstoreservice.Status.OK) {
                Exception ex = new Exception(String.format("The WorkFragment %s executed successfully on Partition %d but result is null for %s",
                                                           wfrag.getFragmentIdList(), this.partitionId, ts));
                if (d) LOG.warn(ex);
                status = Hstoreservice.Status.ABORT_UNEXPECTED;
                error = new SerializableException(ex);
            }
        }
        
        // For single-partition INSERT/UPDATE/DELETE queries, we don't directly
        // execute the SendPlanNode in order to get back the number of tuples that
        // were modified. So we have to rely on the output dependency ids set in the task
        assert(status != Hstoreservice.Status.OK ||
              (status == Hstoreservice.Status.OK && result.size() == wfrag.getFragmentIdCount())) :
           "Got back " + result.size() + " results but was expecting " + wfrag.getFragmentIdCount();
        
        // Make sure that we mark the round as finished before we start sending results
        if (is_local == false) {
            ts.finishRound(this.partitionId);
        }
        
        // -------------------------------
        // LOCAL TRANSACTION
        // -------------------------------
        if (is_dtxn == false) {
            // If the transaction is local, store the result directly in the local TransactionState
            if (status == Hstoreservice.Status.OK) {
                if (t) LOG.trace("Storing " + result.size() + " dependency results locally for successful FragmentTaskMessage");
                LocalTransaction local_ts = (LocalTransaction)ts;
                assert(result.size() == wfrag.getOutputDepIdCount());
                for (int i = 0, cnt = result.size(); i < cnt; i++) {
                    int dep_id = wfrag.getOutputDepId(i);
                    // ts.addResult(result.depIds[i], result.dependencies[i]);
                    if (t) LOG.trace("Storing DependencyId #" + dep_id  + " for " + ts);
                    try {
                        local_ts.addResult(this.partitionId, dep_id, result.dependencies[i]);
                    } catch (Throwable ex) {
                        String msg = String.format("Failed to stored Dependency #%d for %s [idx=%d, fragmentId=%d]",
                                                   dep_id, ts, i, wfrag.getFragmentId(i));
                        LOG.error(msg + "\n" + wfrag.toString());
                        throw new RuntimeException(msg, ex);
                    }
                } // FOR
            } else {
                ts.setPendingError(error);
            }
        }
            
        // -------------------------------
        // REMOTE TRANSACTION
        // -------------------------------
        else {
            if (d) LOG.debug(String.format("Constructing WorkResult %s with %d bytes from partition %d to send back to initial partition %d [status=%s]",
                                                    ts,
                                                    (result != null ? result.size() : null),
                                                    this.partitionId, ts.getBasePartition(),
                                                    status));
            
            RpcCallback<WorkResult> callback = ((RemoteTransaction)ts).getFragmentTaskCallback();
            if (callback == null) {
                LOG.fatal("Unable to send FragmentResponseMessage for " + ts);
                LOG.fatal("Orignal FragmentTaskMessage:\n" + wfrag);
                LOG.fatal(ts.toString());
                throw new RuntimeException("No RPC callback to HStoreSite for " + ts);
            }
            WorkResult response = this.buildWorkResult((RemoteTransaction)ts, result, status, error);
            assert(response != null);
            callback.run(response);
            
        }
    }
    
    /**
     * Executes a FragmentTaskMessage on behalf of some remote site and returns the resulting DependencySet
     * @param wfrag
     * @return
     * @throws Exception
     */
    private DependencySet executeWorkFragment(AbstractTransaction ts, WorkFragment wfrag, ParameterSet parameters[]) throws Exception {
        DependencySet result = null;
        final long undoToken = ts.getLastUndoToken(this.partitionId);
        int fragmentCount = wfrag.getFragmentIdCount();
        long fragmentIds[] = new long[fragmentCount];
        int outputDepIds[] = new int[fragmentCount];
        int inputDepIds[] = new int[fragmentCount]; // Is this ok?

        if (fragmentCount == 0) {
            LOG.warn(String.format("Got a FragmentTask for %s that does not have any fragments?!?", ts));
            return (result);
        }
        
        // Construct arrays given to the EE
        for (int i = 0; i < fragmentCount; i++) {
            fragmentIds[i] = wfrag.getFragmentId(i);
            outputDepIds[i] = wfrag.getOutputDepId(i);
            for (int input_depId : wfrag.getInputDepId(i).getIdsList()) {
                inputDepIds[i] = input_depId; // FIXME!
            } // FOR
        } // FOR
        
        // Input Dependencies
        this.tmp_EEdependencies.clear();
        this.getFragmentInputs(ts, wfrag, this.tmp_EEdependencies);
        
        if (d) {
            LOG.debug(String.format("Getting ready to kick %d fragments to EE for %s", fragmentCount, ts));
            if (t) {
                LOG.trace("FragmentTaskIds: " + Arrays.toString(fragmentIds));
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                for (int i = 0; i < parameters.length; i++) {
                    m.put("Parameter[" + i + "]", parameters[i]);
                } // FOR
                LOG.trace("Parameters:\n" + StringUtil.formatMaps(m));
            }
        }
        
        // -------------------------------
        // SYSPROC FRAGMENTS
        // -------------------------------
        if (ts.isSysProc()) {
            assert(fragmentIds.length == 1);
            long fragment_id = fragmentIds[0];
            assert(fragmentIds.length == parameters.length) :
                String.format("Fragments:%d / Parameters:%d", fragmentIds.length, parameters.length);
            ParameterSet fragmentParams = parameters[0];

            VoltSystemProcedure volt_proc = this.m_registeredSysProcPlanFragments.get(fragment_id);
            if (volt_proc == null) throw new RuntimeException("No sysproc handle exists for FragmentID #" + fragment_id + " :: " + this.m_registeredSysProcPlanFragments);
            
            // HACK: We have to set the TransactionState for sysprocs manually
            volt_proc.setTransactionState(ts);
            ts.markExecNotReadOnly(this.partitionId);
            result = volt_proc.executePlanFragment(ts.getTransactionId(),
                                                   this.tmp_EEdependencies,
                                                   (int)fragment_id,
                                                   fragmentParams,
                                                   this.m_systemProcedureContext);
            if (d) LOG.debug(String.format("Finished executing sysproc fragments for %s\n%s", ts, result));
        // -------------------------------
        // REGULAR FRAGMENTS
        // -------------------------------
        } else {
            result = this.executePlanFragments(ts,
                                               undoToken,
                                               fragmentCount,
                                               fragmentIds,
                                               parameters,
                                               outputDepIds,
                                               inputDepIds,
                                               this.tmp_EEdependencies);
            if (result == null) {
                LOG.warn(String.format("Output DependencySet for %s in %s is null?", Arrays.toString(fragmentIds), ts));
            }
        }
        return (result);
    }
    
    /**
     * Execute a BatcPlan directly on this PartitionExecutor without having to covert it
     * to FragmentTaskMessages first. This is big speed improvement over having to queue things up
     * @param ts
     * @param plan
     * @return
     */
    public VoltTable[] executeLocalPlan(LocalTransaction ts, BatchPlanner.BatchPlan plan, ParameterSet parameterSets[]) {
        long undoToken = HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN;
        
        // If we originally executed this transaction with undo buffers and we have a MarkovEstimate,
        // then we can go back and check whether we want to disable undo logging for the rest of the transaction
        // We can do this regardless of whether the transaction has written anything <-- NOT TRUE!
        if (ts.getEstimatorState() != null && ts.isPredictSinglePartition() && ts.isSpeculative() == false && hstore_conf.site.exec_no_undo_logging) {
            MarkovEstimate est = ts.getEstimatorState().getLastEstimate();
            assert(est != null) : "Got back null MarkovEstimate for " + ts;
            if (est.isAbortable(this.thresholds) || est.isReadOnlyPartition(this.thresholds, this.partitionId) == false) {
                undoToken = this.getNextUndoToken();
            } else if (d) {
                LOG.debug(String.format("Bold! Disabling undo buffers for inflight %s [prob=%f]\n%s\n%s",
                                        ts, est.getAbortProbability(), est, plan.toString()));
            }
        } else if (ts.isPredictReadOnly() == false && hstore_conf.site.exec_no_undo_logging_all == false) {
            undoToken = this.getNextUndoToken();
        }
        ts.fastInitRound(this.partitionId, undoToken);
        ts.setBatchSize(plan.getBatchSize());
      
        int fragmentCount = plan.getFragmentCount();
        long fragmentIds[] = plan.getFragmentIds();
        int output_depIds[] = plan.getOutputDependencyIds();
        int input_depIds[] = plan.getInputDependencyIds();
        
        // Mark that we touched the local partition once for each query in the batch
        // ts.getTouchedPartitions().put(this.partitionId, plan.getBatchSize());
        
        // Only notify other partitions that we're done with them if we're not a single-partition transaction
        if (hstore_conf.site.exec_speculative_execution && ts.isPredictSinglePartition() == false) {
            // TODO: We need to notify the remote HStoreSites that we are done with their partitions
            this.calculateDonePartitions(ts);
        }

        if (t) {
//            StringBuilder sb = new StringBuilder();
//            sb.append("Parameters:");
//            for (int i = 0; i < parameterSets.length; i++) {
//                sb.append(String.format("\n [%02d] %s", i, parameterSets[i].toString()));
//            }
//            LOG.trace(sb.toString());
            LOG.trace(String.format("Txn #%d - BATCHPLAN:\n" +
                     "  fragmentIds:   %s\n" + 
                     "  fragmentCount: %s\n" +
                     "  output_depIds: %s\n" +
                     "  input_depIds:  %s",
                     ts.getTransactionId(),
                     Arrays.toString(plan.getFragmentIds()), plan.getFragmentCount(), Arrays.toString(plan.getOutputDependencyIds()), Arrays.toString(plan.getInputDependencyIds())));
        }
        // NOTE: There are no dependencies that we need to pass in because the entire batch is single-partitioned
        DependencySet result = this.executePlanFragments(ts,
                                                         undoToken,
                                                         fragmentCount,
                                                         fragmentIds,
                                                         parameterSets,
                                                         output_depIds,
                                                         input_depIds,
                                                         null);
        assert(result != null) : "Unexpected null DependencySet for " + ts; 
        if (t) LOG.trace("Output:\n" + result);
        
        ts.fastFinishRound(this.partitionId);
        return (result.dependencies);
    }
    
    /**
     * Execute the given fragment tasks on this site's underlying EE
     * @param ts
     * @param undoToken
     * @param batchSize
     * @param fragmentIds
     * @param parameterSets
     * @param output_depIds
     * @param input_depIds
     * @return
     */
    private DependencySet executePlanFragments(AbstractTransaction ts, long undoToken, int batchSize, long fragmentIds[], ParameterSet parameterSets[], int output_depIds[], int input_depIds[], Map<Integer, List<VoltTable>> input_deps) {
        assert(this.ee != null) : "The EE object is null. This is bad!";
        Long txn_id = ts.getTransactionId();
        
        // *********************************** DEBUG ***********************************
        if (d) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%s - Executing %d fragments [lastTxnId=%d, undoToken=%d]",
                                    ts, batchSize, this.lastCommittedTxnId, undoToken));
            if (t) {
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                m.put("Fragments", Arrays.toString(fragmentIds));
                
                Map<Integer, Object> inner = new ListOrderedMap<Integer, Object>();
                for (int i = 0; i < parameterSets.length; i++)
                    inner.put(i, parameterSets[i].toString());
                m.put("Parameters", inner);
                
                if (input_depIds.length > 0 && input_depIds[0] != HStoreConstants.NULL_DEPENDENCY_ID) {
                    inner = new ListOrderedMap<Integer, Object>();
                    for (int i = 0; i < input_depIds.length; i++) {
                        List<VoltTable> deps = input_deps.get(input_depIds[i]);
                        inner.put(input_depIds[i], (deps != null ? StringUtil.join("\n", deps) : "???"));
                    } // FOR
                    m.put("Input Dependencies", inner);
                }
                m.put("Output Dependencies", Arrays.toString(output_depIds));
                sb.append("\n" + StringUtil.formatMaps(m)); 
            }
            LOG.debug(sb.toString());
        }
        // *********************************** DEBUG ***********************************

        // pass attached dependencies to the EE (for non-sysproc work).
        if (input_deps != null && input_deps.isEmpty() == false) {
            if (d) LOG.debug(String.format("%s - Stashing %d InputDependencies at partition %d",
                                           ts, input_deps.size(), this.partitionId));
//            assert(dependencies.size() == input_depIds.length) : "Expected " + input_depIds.length + " dependencies but we have " + dependencies.size();
            ee.stashWorkUnitDependencies(input_deps);
        }
        
        // Check whether this fragments are read-only
        if (ts.isExecReadOnly(this.partitionId)) {
            boolean readonly = CatalogUtil.areFragmentsReadOnly(this.database, fragmentIds, batchSize); 
            if (readonly == false) {
                if (d) LOG.debug(String.format("%s - Marking txn as not read-only %s", ts, Arrays.toString(fragmentIds))); 
                ts.markExecNotReadOnly(this.partitionId);
            }
            
            // We can do this here because the only way that we're not read-only is if
            // we actually modify data at this partition
            ts.setSubmittedEE(this.partitionId);
        }
        
        DependencySet result = null;
        boolean needs_profiling = (hstore_conf.site.txn_profiling && ts.isExecLocal(this.partitionId));
        if (needs_profiling) ((LocalTransaction)ts).profiler.startExecEE();
        try {
            if (d) LOG.debug(String.format("%s - Executing fragments %s at partition %d",
                                           ts, Arrays.toString(fragmentIds), this.partitionId));
            
            result = this.ee.executeQueryPlanFragmentsAndGetDependencySet(
                            fragmentIds,
                            batchSize,
                            input_depIds,
                            output_depIds,
                            parameterSets,
                            batchSize,
                            txn_id,
                            this.lastCommittedTxnId,
                            undoToken);
            
        } catch (EEException ex) {
            LOG.fatal(String.format("%s - Unrecoverable error in the ExecutionEngine", ts), ex);
            System.exit(1);
        } catch (Throwable ex) {
            new RuntimeException(String.format("%s - Failed to execute PlanFragments: %s", ts, Arrays.toString(fragmentIds)), ex);
        } finally {
            if (needs_profiling) ((LocalTransaction)ts).profiler.stopExecEE();
        }
        
        // *********************************** DEBUG ***********************************
        if (d) {
            if (result != null) {
                LOG.debug(String.format("%s - Finished executing fragments and got back %d results", ts, result.depIds.length));
            } else {
                LOG.warn(String.format("%s - Finished executing fragments but got back null results? That seems bad...", ts));
            }
        }
        // *********************************** DEBUG ***********************************
        return (result);
    }
    
    /**
     * 
     * @param txn_id
     * @param clusterName
     * @param databaseName
     * @param tableName
     * @param data
     * @param allowELT
     * @throws VoltAbortException
     */
    public void loadTable(AbstractTransaction ts, String clusterName, String databaseName, String tableName, VoltTable data, int allowELT) throws VoltAbortException {
        if (cluster == null) {
            throw new VoltProcedure.VoltAbortException("cluster '" + clusterName + "' does not exist");
        }
        if (this.database.getName().equalsIgnoreCase(databaseName) == false) {
            throw new VoltAbortException("database '" + databaseName + "' does not exist in cluster " + clusterName);
        }
        Table table = this.database.getTables().getIgnoreCase(tableName);
        if (table == null) {
            throw new VoltAbortException("table '" + tableName + "' does not exist in database " + clusterName + "." + databaseName);
        }

        ts.setSubmittedEE(this.partitionId);
        ee.loadTable(table.getRelativeIndex(), data,
                     ts.getTransactionId(),
                     lastCommittedTxnId,
                     getNextUndoToken(),
                     allowELT != 0);
    }
    

    /**
     * 
     * @param fresponse
     */
    protected WorkResult buildWorkResult(AbstractTransaction ts, DependencySet result, Hstoreservice.Status status, SerializableException error) {
        WorkResult.Builder builder = WorkResult.newBuilder();
        
        // Partition Id
        builder.setPartitionId(this.partitionId);
        
        // Hstoreservice.Status
        builder.setStatus(status);
        
        // SerializableException 
        if (error != null) {
            int size = error.getSerializedSize();
            BBContainer bc = buffer_pool.acquire(size);
            error.serializeToBuffer(bc.b);
            builder.setError(ByteString.copyFrom(bc.b));
            bc.discard();
        }
        
        // Push dependencies back to the remote partition that needs it
        if (status == Hstoreservice.Status.OK) {
//            FastSerializer fs = new FastSerializer(); // buffer_pool);
            for (int i = 0, cnt = result.size(); i < cnt; i++) {
                DataFragment.Builder outputBuilder = DataFragment.newBuilder();
                outputBuilder.setId(result.depIds[i]);
                try {
//                    if (i > 0) fs.clear();
//                    fs.writeObjectForMessaging(result.dependencies[i]);
                    outputBuilder.addData(ByteString.copyFrom(FastSerializer.serialize(result.dependencies[i])));
                } catch (Exception ex) {
                    throw new RuntimeException(String.format("Failed to serialize output dependency %d for %s", result.depIds[i], ts));
                }
                builder.addOutput(outputBuilder.build());
                if (t) LOG.trace(String.format("Serialized Output Dependency %d for %s\n%s", result.depIds[i], ts, result.dependencies[i]));  
            } // FOR
//            fs.getBBContainer().discard();
        }
        
        return (builder.build());
    }
    
    /**
     * Figure out what partitions this transaction is done with and notify those partitions
     * that they are done
     * @param ts
     */
    private boolean calculateDonePartitions(LocalTransaction ts) {
        final Collection<Integer> ts_done_partitions = ts.getDonePartitions();
        final int ts_done_partitions_size = ts_done_partitions.size();
        Set<Integer> new_done = null;

        TransactionEstimator.State t_state = ts.getEstimatorState();
        if (t_state == null) {
            return (false);
        }
        
        if (d) LOG.debug(String.format("Checking MarkovEstimate for %s to see whether we can notify any partitions that we're done with them [round=%d]",
                                       ts, ts.getCurrentRound(this.partitionId)));
        
        MarkovEstimate estimate = t_state.getLastEstimate();
        assert(estimate != null) : "Got back null MarkovEstimate for " + ts;
        new_done = estimate.getFinishedPartitions(this.thresholds);
        
        if (new_done.isEmpty() == false) { 
            // Note that we can actually be done with ourself, if this txn is only going to execute queries
            // at remote partitions. But we can't actually execute anything because this partition's only 
            // execution thread is going to be blocked. So we always do this so that we're not sending a 
            // useless message
            new_done.remove(this.partitionId);
            
            // Make sure that we only tell partitions that we actually touched, otherwise they will
            // be stuck waiting for a finish request that will never come!
            Collection<Integer> ts_touched = ts.getTouchedPartitions().values();

            // Mark the txn done at this partition if the MarkovEstimate said we were done
            for (Integer p : new_done) {
                if (ts_done_partitions.contains(p) == false && ts_touched.contains(p)) {
                    if (t) LOG.trace(String.format("Marking partition %d as done for %s", p, ts));
                    ts_done_partitions.add(p);
                }
            } // FOR
        }
        return (ts_done_partitions.size() != ts_done_partitions_size);
    }

    /**
     * This site is requesting that the coordinator execute work on its behalf
     * at remote sites in the cluster 
     * @param ftasks
     */
    private void requestWork(LocalTransaction ts, Collection<WorkFragment> tasks, List<ByteString> parameterSets) {
        assert(!tasks.isEmpty());
        assert(ts != null);
        Long txn_id = ts.getTransactionId();

        if (t) LOG.trace(String.format("Wrapping %d WorkFragments into a TransactionWorkRequest for %s", tasks.size(), ts));
        
        // If our transaction was originally designated as a single-partitioned, then we need to make
        // sure that we don't touch any partition other than our local one. If we do, then we need abort
        // it and restart it as multi-partitioned
        boolean need_restart = false;
        boolean predict_singlepartition = ts.isPredictSinglePartition(); 
        Collection<Integer> done_partitions = ts.getDonePartitions();
        
        boolean new_done = false;
        if (hstore_conf.site.exec_speculative_execution) {
            new_done = this.calculateDonePartitions(ts);
        }
        
        // Now we can go back through and start running all of the FragmentTaskMessages that were not blocked
        // waiting for an input dependency. Note that we pack all the fragments into a single
        // CoordinatorFragment rather than sending each FragmentTaskMessage in its own message
        assert(tmp_transactionRequestBuildersMap.isEmpty());
        for (WorkFragment ftask : tasks) {
            assert(!ts.isBlocked(ftask));
            
            int target_partition = ftask.getPartitionId();
            int target_site = hstore_site.getSiteIdForPartitionId(target_partition);
            Set<Integer> input_dep_ids = tmp_transactionRequestBuilderInputs.get(target_site);
            
            // Make sure that this isn't a single-partition txn trying to access a remote partition
            if (predict_singlepartition && target_partition != this.partitionId) {
                if (d) LOG.debug(String.format("%s on partition %d is suppose to be single-partitioned, but it wants to execute a fragment on partition %d",
                                               ts, this.partitionId, target_partition));
                need_restart = true;
                break;
            }
            // Make sure that this txn isn't trying ot access a partition that we said we were
            // done with earlier
            else if (done_partitions.contains(target_partition)) {
                if (d) LOG.debug(String.format("%s on partition %d was marked as done on partition %d but now it wants to go back for more!",
                                               ts, this.partitionId, target_partition));
                need_restart = true;
                break;
            }
            // Make sure we at least have something to do!
            else if (ftask.getFragmentIdCount() == 0) {
                LOG.warn(String.format("%s - Trying to send a WorkFragment request with 0 fragments", ts));
                continue;
            }
            
            // Get the TransactionWorkRequest.Builder for the remote HStoreSite
            // We will use this store our serialized input dependencies
            TransactionWorkRequest.Builder request = tmp_transactionRequestBuildersMap.get(target_site);
            if (request == null) {
                request = TransactionWorkRequest.newBuilder()
                                        .setTransactionId(txn_id)
                                        .setSourcePartition(this.partitionId)
                                        .setSysproc(ts.isSysProc())
                                        .addAllDonePartition(done_partitions);
                tmp_transactionRequestBuildersMap.put(target_site, request);
                input_dep_ids.clear();
            }
            
            // Also keep track of what Statements they are executing so that we know
            // we need to send over the wire to them.
            Set<Integer> stmt_indexes = tmp_transactionRequestBuildersParameters.get(target_site);
            assert(stmt_indexes != null);
            CollectionUtil.addAll(stmt_indexes, ftask.getStmtIndexList());
            
            // Input Dependencies
            if (ftask.getNeedsInput()) {
                if (d) LOG.debug("Retrieving input dependencies for " + ts);
                
                tmp_removeDependenciesMap.clear();
                this.getFragmentInputs(ts, ftask, tmp_removeDependenciesMap);

//                if (t) LOG.trace(String.format("%s - Attaching %d dependencies to %s", ts, this.tmp_removeDependenciesMap.size(), ftask));
                FastSerializer fs = null;
                for (Entry<Integer, List<VoltTable>> e : tmp_removeDependenciesMap.entrySet()) {
                    if (input_dep_ids.contains(e.getKey())) continue;

                    if (d) LOG.debug(String.format("%s - Attaching %d input dependencies to be sent to %s",
                                     ts, e.getValue().size(), HStoreSite.formatSiteName(target_site)));
                    DataFragment.Builder dBuilder = DataFragment.newBuilder();
                    dBuilder.setId(e.getKey());                    
                    for (VoltTable vt : e.getValue()) {
//                        fs = new FastSerializer();
                        if (fs == null) fs = new FastSerializer(); // buffer_pool);
                        else fs.clear();
                        try {
                            fs.writeObject(vt);
                            dBuilder.addData(ByteString.copyFrom(fs.getBuffer()));
                        } catch (Exception ex) {
                            throw new RuntimeException(String.format("Failed to serialize input dependency %d for %s", e.getKey(), ts));
                        }
                        if (d)
                            LOG.debug(String.format("%s - Storing %d rows for InputDependency %d to send to partition %d [bytes=%d]",
                                                    ts, vt.getRowCount(), e.getKey(), ftask.getPartitionId(),
                                                    CollectionUtil.last(dBuilder.getDataList()).size()));
                    } // FOR
                    input_dep_ids.add(e.getKey());
                    request.addAttached(dBuilder.build());
                } // FOR
//                if (fs != null) fs.getBBContainer().discard();
            }
            
            request.addFragments(ftask);
        } // FOR (tasks)
        
        // Bad mojo! We need to throw a MispredictionException so that the VoltProcedure
        // will catch it and we can propagate the error message all the way back to the HStoreSite
        if (need_restart) {
            if (t) LOG.trace(String.format("Aborting %s because it was mispredicted", ts));
            // This is kind of screwy because we don't actually want to send the touched partitions
            // histogram because VoltProcedure will just do it for us...
            throw new MispredictionException(txn_id, null);
        }

        // Stick on the ParameterSets that each site needs into the TransactionWorkRequest
        for (Integer target_site : tmp_transactionRequestBuildersMap.keySet()) {
            TransactionWorkRequest.Builder request = tmp_transactionRequestBuildersMap.get(target_site);
            assert(request != null);
            Set<Integer> stmt_indexes = tmp_transactionRequestBuildersParameters.get(target_site);
            assert(stmt_indexes != null);
            assert(stmt_indexes.isEmpty() == false);
            for (int i = 0, cnt = parameterSets.size(); i < cnt; i++) {
                request.addParameterSets(stmt_indexes.contains(i) ? parameterSets.get(i) : ByteString.EMPTY); 
            } // FOR
        } // FOR
        
        
        // Bombs away!
        this.hstore_coordinator.transactionWork(ts, tmp_transactionRequestBuildersMap, this.request_work_callback);
        if (d) LOG.debug(String.format("Work request for %d fragments was sent to %d remote HStoreSites for %s",
                                       tasks.size(), tmp_transactionRequestBuildersMap.size(), ts));

        // TODO: We need to check whether we need to notify other HStoreSites that we didn't send
        // a new FragmentTaskMessage to that we are done with their partitions
        if (new_done) {
            
        }
        
        // We want to clear out our temporary map here so that we don't have to do it
        // the next time we need to use this
        tmp_transactionRequestBuildersMap.clear();
    }

    /**
     * Execute the given tasks and then block the current thread waiting for the list of dependency_ids to come
     * back from whatever it was we were suppose to do...
     * This is the slowest way to execute a bunch of WorkFragments and therefore should only be invoked
     * for batches that need to access non-local Partitions
     * @param ts
     * @param fragments
     * @param parameters
     * @return
     */
    public VoltTable[] dispatchWorkFragments(LocalTransaction ts, Collection<WorkFragment> fragments, ParameterSet parameters[]) {
        // *********************************** DEBUG ***********************************
        if (d) {
            LOG.debug(String.format("%s - Preparing to dispatch %d messages and wait for the results",
                                             ts, fragments.size()));
            if (t) {
                StringBuilder sb = new StringBuilder();
                sb.append(ts + " WorkFragments:\n");
                for (WorkFragment fragment : fragments) {
                    sb.append(StringUtil.box(fragment.toString()) + "\n");
                } // FOR
                sb.append(ts + " ParameterSets:\n");
                for (ParameterSet ps : parameters) {
                    sb.append(ps + "\n");
                } // FOR
                LOG.trace(sb);
            }
        }
        // *********************************** DEBUG *********************************** 
        
        // OPTIONAL: Check to make sure that this request is valid 
        //  (1) At least one of the WorkFragments needs to be executed on a remote partition
        //  (2) All of the PlanFragments ids in the WorkFragments match this txn's Procedure
        if (hstore_conf.site.exec_validate_work && ts.isSysProc() == false) {
            LOG.warn(String.format("%s - Checking whether all of the WorkFragments are valid", ts));
            boolean has_remote = false; 
            for (WorkFragment frag : fragments) {
                if (frag.getPartitionId() != this.partitionId) {
                    has_remote = true;
                }
                for (int frag_id : frag.getFragmentIdList()) {
                    PlanFragment catalog_frag = CatalogUtil.getPlanFragment(database, frag_id);
                    Statement catalog_stmt = catalog_frag.getParent();
                    assert(catalog_stmt != null);
                    Procedure catalog_proc = catalog_stmt.getParent();
                    if (catalog_proc.equals(ts.getProcedure()) == false) {
                        LOG.warn(ts.debug() + "\n" + fragments + "\n---- INVALID ----\n" + frag);
                        throw new RuntimeException(String.format("%s - Unexpected %s", ts, catalog_frag.fullName()));
                    }
                }
            } // FOR
            if (has_remote == false) {
                LOG.warn(ts.debug() + "\n" + fragments);
                throw new RuntimeException(String.format("%s - Trying to execute all local single-partition queries using the slow-path!", ts));
            }
        }
        
        // We have to store all of the tasks in the TransactionState before we start executing, otherwise
        // there is a race condition that a task with input dependencies will start running as soon as we
        // get one response back from another executor
        ts.initRound(this.partitionId, this.getNextUndoToken());
        ts.setBatchSize(parameters.length);
        boolean first = true;
        final boolean predict_singlePartition = ts.isPredictSinglePartition();
        boolean serializedParams = false;
        CountDownLatch latch = null;
        
        // Attach the ParameterSets to our transaction handle so that anybody on this HStoreSite
        // can access them directly without needing to deserialize them from the WorkFragments
        ts.attachParameterSets(parameters);
        
        // Now if we have some work sent out to other partitions, we need to wait until they come back
        // In the first part, we wait until all of our blocked FragmentTaskMessages become unblocked
        LinkedBlockingDeque<Collection<WorkFragment>> queue = ts.getUnblockedWorkFragmentsQueue();

        boolean all_local = true;
        boolean is_localSite;
        boolean is_localPartition;
        int num_localPartition = 0;
        int num_localSite = 0;
        int num_remote = 0;
        int total = 0;
        
        // Run through this loop if:
        //  (1) This is our first time in the loop (first == true)
        //  (2) If we know that there are still messages being blocked
        //  (3) If we know that there are still unblocked messages that we need to process
        //  (4) The latch for this round is still greater than zero
        while (first == true || ts.stillHasWorkFragments() || (latch != null && latch.getCount() > 0)) {
            if (t) LOG.trace(String.format("%s - [first=%s, stillHasWorkFragments=%s, latch=%s]",
                                           ts, first, ts.stillHasWorkFragments(), queue.size(), latch));
            
            // If this is the not first time through the loop, then poll the queue to get our list of fragments
            if (first == false) {
                all_local = true;
                is_localSite = false;
                is_localPartition = false;
                num_localPartition = 0;
                num_localSite = 0;
                num_remote = 0;
                total = 0;
                
                if (t) LOG.trace(String.format("%s - Waiting for unblocked tasks on partition %d", ts, this.partitionId));
                if (hstore_conf.site.txn_profiling) ts.profiler.startExecDtxnWork();
                try {
                    fragments = queue.takeFirst(); // BLOCKING
                } catch (InterruptedException ex) {
                    if (this.hstore_site.isShuttingDown() == false) {
                        LOG.error(String.format("%s - We were interrupted while waiting for blocked tasks", ts), ex);
                    }
                    return (null);
                } finally {
                    if (hstore_conf.site.txn_profiling) ts.profiler.stopExecDtxnWork();
                }
            }
            assert(fragments != null);
            
            // If the list to fragments unblock is empty, then we 
            // know that we have dispatched all of the WorkFragments for the
            // transaction's current SQLStmt batch. That means we can just wait 
            // until all the results return to us.
            if (fragments.isEmpty()) {
                if (t) LOG.trace(String.format("%s - Got an empty list of WorkFragments. Blocking until dependencies arrive", ts)); 
                break;
            }

            this.tmp_localWorkFragmentList.clear();
            if (predict_singlePartition == false) {
                this.tmp_remoteFragmentList.clear();
                this.tmp_localSiteFragmentList.clear();
            }
            
            // FAST PATH: Assume everything is local
            if (predict_singlePartition) {
                for (WorkFragment ftask : fragments) {
                    if (first == false || ts.addWorkFragment(ftask) == false) {
                        this.tmp_localWorkFragmentList.add(ftask);
                        total++;
                        num_localPartition++;
                    }
                } // FOR
                
                // We have to tell the TransactinState to start the round before we send off the
                // FragmentTasks for execution, since they might start executing locally!
                if (first) {
                    ts.startRound(this.partitionId);
                    latch = ts.getDependencyLatch();
                }
                
                for (WorkFragment fragment : this.tmp_localWorkFragmentList) {
                    if (d) LOG.debug(String.format("Got unblocked FragmentTaskMessage for %s. Executing locally...", ts));
                    assert(fragment.getPartitionId() == this.partitionId) :
                        String.format("Trying to process FragmentTaskMessage for %s on partition %d but it should have been sent to partition %d [singlePartition=%s]\n%s",
                                      ts, this.partitionId, fragment.getPartitionId(), predict_singlePartition, fragment);
                    ParameterSet fragmentParams[] = this.getFragmentParameters(ts, fragment, parameters);
                    this.processWorkFragment(ts, fragment, fragmentParams);
//                    read_only = read_only && ftask.isReadOnly();
                } // FOR
                
            // SLOW PATH: Mixed local and remote messages
            } else {
                
                // Look at each task and figure out whether it should be executed remotely or locally
                for (WorkFragment ftask : fragments) {
                    int partition = ftask.getPartitionId();
                    is_localSite = localPartitionIds.contains(partition);
                    is_localPartition = (is_localSite && partition == this.partitionId);
                    all_local = all_local && is_localPartition;
                    if (first == false || ts.addWorkFragment(ftask) == false) {
                        total++;
                        if (is_localPartition) {
                            this.tmp_localWorkFragmentList.add(ftask);
                            num_localPartition++;
                        } else if (is_localSite) {
                            this.tmp_localSiteFragmentList.add(ftask);
                            num_localSite++;
                        } else {
                            this.tmp_remoteFragmentList.add(ftask);
                            num_remote++;
                        }
                    }
                } // FOR
                assert(total == (num_remote + num_localSite + num_localPartition));
                if (num_localPartition == 0 && num_localSite == 0 && num_remote == 0) {
                    throw new RuntimeException(String.format("Deadlock! All tasks for %s are blocked waiting on input!", ts));
                }

                // We have to tell the TransactinState to start the round before we send off the
                // FragmentTasks for execution, since they might start executing locally!
                if (first) {
                    ts.startRound(this.partitionId);
                    latch = ts.getDependencyLatch();
                }
        
                // Now request the fragments that aren't local
                // We want to push these out as soon as possible
                if (num_remote > 0) {
                    // We only need to serialize the ParameterSets once
                    if (serializedParams == false) {
                        if (hstore_conf.site.txn_profiling) ts.profiler.startSerialization();
                        tmp_serializedParams.clear();
                        FastSerializer fs = new FastSerializer();
                        for (int i = 0; i < parameters.length; i++) {
                            ParameterSet ps = parameters[i];
                            if (ps == null) tmp_serializedParams.add(ByteString.EMPTY);
                            fs.clear();
                            try {
                                ps.writeExternal(fs);
                                ByteString bs = ByteString.copyFrom(fs.getBuffer());
                                tmp_serializedParams.add(bs);
                            } catch (Exception ex) {
                                throw new RuntimeException("Failed to serialize ParameterSet " + i + " for " + ts, ex);
                            }
                        } // FOR
                        if (hstore_conf.site.txn_profiling) ts.profiler.stopSerialization();
                    }
                    if (d) LOG.debug(String.format("Requesting %d FragmentTaskMessages to be executed on remote partitions for %s", num_remote, ts));
                    this.requestWork(ts, tmp_remoteFragmentList, tmp_serializedParams);
                }
                
                // Then dispatch the task that are needed at the same HStoreSite but 
                // at a different partition than this one
                if (num_localSite > 0) {
                    if (d) LOG.debug(String.format("Executing %d FragmentTaskMessages on local site's partitions for %s",
                                                   num_localSite, ts));
                    for (WorkFragment fragment : this.tmp_localSiteFragmentList) {
                        FragmentTaskMessage ftask = ts.getFragmentTaskMessage(fragment);
                        hstore_site.getPartitionExecutor(fragment.getPartitionId()).queueWork(ts, ftask);
                    } // FOR
                }
        
                // Then execute all of the tasks need to access the partitions at this HStoreSite
                // We'll dispatch the remote-partition-local-site fragments first because they're going
                // to need to get queued up by at the other PartitionExecutors
                if (num_localPartition > 0) {
                    if (d) LOG.debug(String.format("Executing %d FragmentTaskMessages on local partition for %s",
                                                   num_localPartition, ts));
                    for (WorkFragment fragment : this.tmp_localWorkFragmentList) {
                        ParameterSet fragmentParams[] = this.getFragmentParameters(ts, fragment, parameters);
                        this.processWorkFragment(ts, fragment, fragmentParams);
                    } // FOR
                }
            }
            if (t)
                LOG.trace(String.format("%s - Dispatched %d WorkFragments [remoteSite=%d, localSite=%d, localPartition=%d]",
                          ts, total, num_remote, num_localSite, num_localPartition));
            first = false;
        } // WHILE
        if (t)
            LOG.trace(String.format("%s - BREAK OUT [first=%s, stillHasWorkFragments=%s, latch=%s]",
                      ts, first, ts.stillHasWorkFragments(), latch));
//        assert(ts.stillHasWorkFragments() == false) :
//            String.format("Trying to block %s before all of its WorkFragments have been dispatched!\n%s\n%s",
//                          ts,
//                          StringUtil.join("** ", "\n", tempDebug),
//                          this.getVoltProcedure(ts.getProcedureName()).getLastBatchPlan());
                
        // Now that we know all of our FragmentTaskMessages have been dispatched, we can then
        // wait for all of the results to come back in.
        if (latch == null) latch = ts.getDependencyLatch();
        if (latch.getCount() > 0) {
            if (d) {
                LOG.debug(String.format("%s - All blocked messages dispatched. Waiting for %d dependencies", ts, latch.getCount()));
                if (t) LOG.trace(ts.toString());
            }
            if (hstore_conf.site.txn_profiling) ts.profiler.startExecDtxnWork();
            boolean done = false;
            try {
                done = latch.await(hstore_conf.site.exec_response_timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                if (this.hstore_site.isShuttingDown() == false) {
                    LOG.error(String.format("%s - We were interrupted while waiting for results", ts), ex);
                }
                return (null);
            } catch (Throwable ex) {
                new RuntimeException(String.format("Fatal error for %s while waiting for results", ts), ex);
            } finally {
                if (hstore_conf.site.txn_profiling) ts.profiler.stopExecDtxnWork();
            }
            if (done == false && this.isShuttingDown() == false) {
                LOG.warn(String.format("Still waiting for responses for %s after %d ms [latch=%d]\n%s",
                                                ts, hstore_conf.site.exec_response_timeout, latch.getCount(), ts.debug()));
                LOG.warn("Procedure Parameters:\n" + ts.getInvocation().getParams());
                hstore_conf.site.exec_profiling = true;
                LOG.warn(hstore_site.statusSnapshot());
                
                throw new RuntimeException("PartitionResponses for " + ts + " never arrived!");
            }
        }
        
        // IMPORTANT: Check whether the fragments failed somewhere and we got a response with an error
        // We will rethrow this so that it pops the stack all the way back to VoltProcedure.call()
        // where we can generate a message to the client 
        if (ts.hasPendingError()) {
            if (d) LOG.warn(String.format("%s was hit with a %s", ts, ts.getPendingError().getClass().getSimpleName()));
            throw ts.getPendingError();
        }
        
        // IMPORTANT: Don't try to check whether we got back the right number of tables because the batch
        // may have hit an error and we didn't execute all of them.
        VoltTable results[] = ts.getResults();
        ts.finishRound(this.partitionId);
         if (d) {
            if (t) LOG.trace(ts + " is now running and looking for love in all the wrong places...");
            LOG.debug(ts + " is returning back " + results.length + " tables to VoltProcedure");
        }
        return (results);
    }

    // ---------------------------------------------------------------
    // COMMIT + ABORT METHODS
    // ---------------------------------------------------------------

    /**
     * Queue a speculatively executed transaction to send its ClientResponseImpl message
     */
    private void queueClientResponse(LocalTransaction ts, ClientResponseImpl cresponse) {
        if (d) LOG.debug(String.format("Queuing ClientResponse for %s [handle=%s, status=%s]",
                                       ts, ts.getClientHandle(), cresponse.getStatus()));
        assert(ts.isPredictSinglePartition() == true) :
            String.format("Specutatively executed multi-partition %s [mode=%s, status=%s]",
                          ts, this.current_execMode, cresponse.getStatus());
        assert(ts.isSpeculative() == true) :
            String.format("Queuing ClientResponse for non-specutative %s [mode=%s, status=%s]",
                          ts, this.current_execMode, cresponse.getStatus());
        assert(cresponse.getStatus() != Hstoreservice.Status.ABORT_MISPREDICT) : 
            String.format("Trying to queue ClientResponse for mispredicted %s [mode=%s, status=%s]",
                          ts, this.current_execMode, cresponse.getStatus());
        assert(this.current_execMode != ExecutionMode.COMMIT_ALL) :
            String.format("Queuing ClientResponse for %s when in non-specutative mode [mode=%s, status=%s]",
                          ts, this.current_execMode, cresponse.getStatus());

        ts.setClientResponse(cresponse);
        this.queued_responses.add(ts);

        if (d) LOG.debug("Total # of Queued Responses: " + this.queued_responses.size());
    }
    
    /**
     * For the given transaction's ClientResponse, figure out whether we can send it back to the client
     * right now or whether we need to initiate two-phase commit.
     * @param ts
     * @param cresponse
     */
    public void processClientResponse(LocalTransaction ts, ClientResponseImpl cresponse) {
        // IMPORTANT: If we executed this locally and only touched our partition, then we need to commit/abort right here
        // 2010-11-14: The reason why we can do this is because we will just ignore the commit
        // message when it shows from the Dtxn.Coordinator. We should probably double check with Evan on this...
        boolean is_singlepartitioned = ts.isPredictSinglePartition();
        Hstoreservice.Status status = cresponse.getStatus();

        if (d) {
            LOG.debug(String.format("Processing ClientResponse for %s at partition %d [handle=%d, status=%s, singlePartition=%s, local=%s]",
                                    ts, this.partitionId, cresponse.getClientHandle(), status,
                                    ts.isPredictSinglePartition(), ts.isExecLocal(this.partitionId)));
            if (t) {
                LOG.trace(ts + " Touched Partitions: " + ts.getTouchedPartitions().values());
                LOG.trace(ts + " Done Partitions: " + ts.getDonePartitions());
            }
        }
        
        // ALL: Single-Partition Transactions
        if (is_singlepartitioned) {
            // Commit or abort the transaction
            this.finishWork(ts, (status == Hstoreservice.Status.OK));
            
            // Then send the result back to the client!
            if (hstore_conf.site.exec_postprocessing_thread) {
                if (t) LOG.trace(String.format("%s - Sending ClientResponse to post-processing thread [status=%s]",
                                               ts, cresponse.getStatus()));
                hstore_site.queueClientResponse(this, ts, cresponse);
            } else {
                hstore_site.sendClientResponse(ts, cresponse);
                hstore_site.completeTransaction(ts.getTransactionId(), status);
            }
        } 
        // COMMIT: Distributed Transaction
        else if (status == Hstoreservice.Status.OK) {
            // Store the ClientResponse in the TransactionPrepareCallback so that
            // when we get all of our 
            TransactionPrepareCallback callback = ts.getTransactionPrepareCallback();
            assert(callback != null) : "Missing TransactionPrepareCallback for " + ts + " [initialized=" + ts.isInitialized() + "]";
            callback.setClientResponse(cresponse);
            
            // We have to send a prepare message to all of our remote HStoreSites
            // We want to make sure that we don't go back to ones that we've already told
            Collection<Integer> predictPartitions = ts.getPredictTouchedPartitions();
            Collection<Integer> donePartitions = ts.getDonePartitions();
            Collection<Integer> partitions = CollectionUtils.subtract(predictPartitions, donePartitions);
            
            if (hstore_conf.site.txn_profiling) ts.profiler.startPostPrepare();
            this.hstore_coordinator.transactionPrepare(ts, ts.getTransactionPrepareCallback(), partitions);
            
            if (hstore_conf.site.exec_speculative_execution) {
                this.setExecutionMode(ts, ts.isExecReadOnly(this.partitionId) ? ExecutionMode.COMMIT_READONLY : ExecutionMode.COMMIT_NONE);
            } else {
                this.setExecutionMode(ts, ExecutionMode.DISABLED);                  
            }

        }
        // ABORT: Distributed Transaction
        else {
            // Send back the result to the client right now, since there's no way 
            // that we're magically going to be able to recover this and get them a result
            // This has to come before the network messages above because this will clean-up the 
            // LocalTransaction state information
            this.hstore_site.sendClientResponse(ts, cresponse);
            
            // Then send a message all the partitions involved that the party is over
            // and that they need to abort the transaction. We don't actually care when we get the
            // results back because we'll start working on new txns right away.
            if (hstore_conf.site.txn_profiling) ts.profiler.startPostFinish();
            TransactionFinishCallback finish_callback = ts.initTransactionFinishCallback(status);
            this.hstore_coordinator.transactionFinish(ts, status, finish_callback);
        }
    }
        
    /**
     * Internal call to abort/commit the transaction
     * @param ts
     * @param commit
     */
    private void finishWork(AbstractTransaction ts, boolean commit) {
        assert(ts.isFinishedEE(this.partitionId) == false) :
            String.format("Trying to commit %s twice at partition %d", ts, this.partitionId);
        
        // This can be null if they haven't submitted anything
        long undoToken = ts.getLastUndoToken(this.partitionId);
        
        // Only commit/abort this transaction if:
        //  (1) We have an ExecutionEngine handle
        //  (2) We have the last undo token used by this transaction
        //  (3) The transaction was executed with undo buffers
        //  (4) The transaction actually submitted work to the EE
        //  (5) The transaction modified data at this partition
        if (this.ee != null && ts.hasSubmittedEE(this.partitionId) && ts.isExecReadOnly(this.partitionId) == false && undoToken != -1) {
            if (undoToken == HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN) {
                if (commit == false) {
                    LOG.fatal(ts.debug());
                    this.crash(new RuntimeException("TRYING TO ABORT TRANSACTION WITHOUT UNDO LOGGING: "+ ts));
                }
                if (d) LOG.debug("<FinishWork> undoToken == HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN");
            } else {
                boolean needs_profiling = (hstore_conf.site.txn_profiling && ts.isExecLocal(this.partitionId) && ts.isPredictSinglePartition());
                if (needs_profiling) ((LocalTransaction)ts).profiler.startPostEE();
                if (commit) {
                    if (d) LOG.debug(String.format("Committing %s at partition=%d [lastTxnId=%d, undoToken=%d, submittedEE=%s]",
                                                   ts, this.partitionId, this.lastCommittedTxnId, undoToken, ts.hasSubmittedEE(this.partitionId)));
                    this.ee.releaseUndoToken(undoToken);
    
                // Evan says that txns will be aborted LIFO. This means the first txn that
                // we get in abortWork() will have a the greatest undoToken, which means that 
                // it will automagically rollback all other outstanding txns.
                // I'm lazy/tired, so for now I'll just rollback everything I get, but in theory
                // we should be able to check whether our undoToken has already been rolled back
                } else {
                    if (d) LOG.debug(String.format("Aborting %s at partition=%d [lastTxnId=%d, undoToken=%d, submittedEE=%s]",
                                                   ts, this.partitionId, this.lastCommittedTxnId, undoToken, ts.hasSubmittedEE(this.partitionId)));
                    this.ee.undoUndoToken(undoToken);
                }
                if (needs_profiling) ((LocalTransaction)ts).profiler.stopPostEE();
            }
        }
        
        // We always need to do the following things regardless if we hit up the EE or not
        if (commit) this.lastCommittedTxnId = ts.getTransactionId();
        ts.setFinishedEE(this.partitionId);
    }
    
    /**
     * The coordinator is telling our site to abort/commit the txn with the
     * provided transaction id. This method should only be used for multi-partition transactions, because
     * it will do some extra work for speculative execution
     * @param txn_id
     * @param commit If true, the work performed by this txn will be commited. Otherwise it will be aborted
     */
    public void finishTransaction(AbstractTransaction ts, boolean commit) {
        if (d) LOG.debug(String.format("%s - Processing finishWork request at partition %d", ts, this.partitionId));
        if (this.current_dtxn != ts) {  
            return;
        }
        assert(this.current_dtxn == ts) : "Expected current DTXN to be " + ts + " but it was " + this.current_dtxn;
        
        this.finishWork(ts, commit);
        
        // Check whether this is the response that the speculatively executed txns have been waiting for
        // We could have turned off speculative execution mode beforehand 
        if (d) LOG.debug(String.format("Attempting to unmark %s as the current DTXN at partition %d and setting execution mode to %s",
                                       this.current_dtxn, this.partitionId, ExecutionMode.COMMIT_ALL));
        exec_lock.lock();
        try {
            // Resetting the current_dtxn variable has to come *before* we change the execution mode
            this.setCurrentDtxn(null);
            this.setExecutionMode(ts, ExecutionMode.COMMIT_ALL);
            
            // We can always commit our boys no matter what if we know that this multi-partition txn 
            // was read-only at the given partition
            if (hstore_conf.site.exec_speculative_execution) {
                if (d) LOG.debug(String.format("Turning off speculative execution mode at partition %d because %s is finished",
                                               this.partitionId, ts));
                Boolean readonly = ts.isExecReadOnly(this.partitionId);
                this.releaseQueuedResponses(readonly != null && readonly == true ? true : commit);
            }
            if(d) LOG.debug("I am trying to releaseBlocked Transaction");
            // Release blocked transactions
            this.releaseBlockedTransactions(ts, false);
        } catch (Throwable ex) {
            throw new RuntimeException(String.format("Failed to finish %s at partition %d", ts, this.partitionId), ex);
        } finally {
            exec_lock.unlock();
        } // SYNCH
//        if (d) LOG.debug(String.format("%s is releasing DTXN lock [queueSize=%d, waitingLock=%d]",
//                                       ts, this.work_queue.size(), this.dtxn_lock.getQueueLength()));
//         this.dtxn_lock.release();
        
        // HACK: If this is a RemoteTransaction, invoke the cleanup callback
        if (ts instanceof RemoteTransaction) {
            ((RemoteTransaction)ts).getCleanupCallback().run(this.partitionId);
        } else {
            TransactionFinishCallback finish_callback = ((LocalTransaction)ts).getTransactionFinishCallback();
            if (t)
                LOG.trace(String.format("Notifying %s that %s is finished at partition %d",
                                        finish_callback.getClass().getSimpleName(), ts, this.partitionId));
            finish_callback.decrementCounter(1);
        }
        
    }    
    /**
     * 
     * @param txn_id
     * @param p
     */
    private void releaseBlockedTransactions(AbstractTransaction ts, boolean speculative) {
        if (this.current_blockedTxns.isEmpty() == false) {
            if (d) LOG.debug(String.format("Attempting to release %d blocked transactions at partition %d because of %s",
                                           this.current_blockedTxns.size(), this.partitionId, ts));
            int released = 0;
            for (TransactionInfoBaseMessage msg : this.current_blockedTxns) {
                this.work_queue.add(msg);
                released++;
            } // FOR
            this.current_blockedTxns.clear();
            if (d) LOG.debug(String.format("Released %d blocked transactions at partition %d because of %s",
                                         released, this.partitionId, ts));
        }
        assert(this.current_blockedTxns.isEmpty());
    }
    
    /**
     * Commit/abort all of the queue transactions that were specutatively executed and waiting for
     * their responses to be sent back to the client
     * @param commit
     */
    private void releaseQueuedResponses(boolean commit) {
        // First thing we need to do is get the latch that will be set by any transaction
        // that was in the middle of being executed when we were called
        if (d) LOG.debug(String.format("Checking waiting/blocked transactions at partition %d [currentMode=%s]",
                                       this.partitionId, this.current_execMode));
        
        if (this.queued_responses.isEmpty()) {
            if (d) LOG.debug(String.format("No speculative transactions to commit at partition %d. Ignoring...", this.partitionId));
            return;
        }
        
        // Ok now at this point we can access our queue send back all of our responses
        if (d) LOG.debug(String.format("%s %d speculatively executed transactions on partition %d",
                                       (commit ? "Commiting" : "Aborting"), this.queued_responses.size(), this.partitionId));

        // Loop backwards through our queued responses and find the latest txn that 
        // we need to tell the EE to commit. All ones that completed before that won't
        // have to hit up the EE.
        LocalTransaction ts = null;
        boolean ee_commit = true;
        int skip_commit = 0;
        int aborted = 0;
        while ((ts = (hstore_conf.site.exec_queued_response_ee_bypass ? this.queued_responses.pollLast() : this.queued_responses.pollFirst())) != null) {
            ClientResponseImpl cr = ts.getClientResponse();
            // 2011-07-02: I have no idea how this could not be stopped here, but for some reason
            // I am getting a random error.
            // FIXME if (hstore_conf.site.txn_profiling && ts.profiler.finish_time.isStopped()) ts.profiler.finish_time.start();
            
            // If the multi-p txn aborted, then we need to abort everything in our queue
            // Change the status to be a MISPREDICT so that they get executed again
            if (commit == false) {
                cr.setStatus(Hstoreservice.Status.ABORT_MISPREDICT);
                ts.setPendingError(new MispredictionException(ts.getTransactionId(), ts.getTouchedPartitions()), false);
                aborted++;
                
            // Optimization: Check whether the last element in the list is a commit
            // If it is, then we know that we don't need to tell the EE about all the ones that executed before it
            } else if (hstore_conf.site.exec_queued_response_ee_bypass) {
                // Don't tell the EE that we committed
                if (ee_commit == false) {
                    if (t) LOG.trace(String.format("Bypassing EE commit for %s [undoToken=%d]", ts, ts.getLastUndoToken(this.partitionId)));
                    ts.unsetSubmittedEE(this.partitionId);
                    skip_commit++;
                    
                } else if (ee_commit && cr.getStatus() == Hstoreservice.Status.OK) {
                    if (t) LOG.trace(String.format("Committing %s but will bypass all other successful transactions [undoToken=%d]", ts, ts.getLastUndoToken(this.partitionId)));
                    ee_commit = false;
                }
            }
            
            try {
                if (hstore_conf.site.exec_postprocessing_thread) {
                    if (t) LOG.trace(String.format("Passing queued ClientResponse for %s to post-processing thread [status=%s]", ts, cr.getStatus()));
                    hstore_site.queueClientResponse(this, ts, cr);
                } else {
                    if (t) LOG.trace(String.format("Sending queued ClientResponse for %s back directly [status=%s]", ts, cr.getStatus()));
                    this.processClientResponse(ts, cr);
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Failed to complete queued " + ts, ex);
            }
        } // WHILE
        if (d && skip_commit > 0 && hstore_conf.site.exec_queued_response_ee_bypass) {
            LOG.debug(String.format("Fast Commit EE Bypass Optimization [skipped=%d, aborted=%d]", skip_commit, aborted));
        }
        return;
    }
    
    // ---------------------------------------------------------------
    // SHUTDOWN METHODS
    // ---------------------------------------------------------------
    
    /**
     * Cause this PartitionExecutor to make the entire HStore cluster shutdown
     * This won't return!
     */
    public synchronized void crash(Throwable ex) {
        LOG.warn(String.format("PartitionExecutor for Partition #%d is crashing", this.partitionId), ex);
        assert(this.hstore_coordinator != null);
        this.hstore_coordinator.shutdownCluster(ex); // This won't return
    }
    
    @Override
    public boolean isShuttingDown() {
        return (this.hstore_site.isShuttingDown()); // shutdown_state == State.PREPARE_SHUTDOWN || this.shutdown_state == State.SHUTDOWN);
    }
    
    @Override
    public void prepareShutdown(boolean error) {
        shutdown_state = Shutdownable.ShutdownState.PREPARE_SHUTDOWN;
    }
    
    /**
     * Somebody from the outside wants us to shutdown
     */
    public synchronized void shutdown() {
        if (this.shutdown_state == ShutdownState.SHUTDOWN) {
            if (d) LOG.debug(String.format("Partition #%d told to shutdown again. Ignoring...", this.partitionId));
            return;
        }
        this.shutdown_state = ShutdownState.SHUTDOWN;
        
        if (d) LOG.debug(String.format("Shutting down PartitionExecutor for Partition #%d", this.partitionId));
        
        // Clear the queue
        this.work_queue.clear();
        
        // Make sure we shutdown our threadpool
        // this.thread_pool.shutdownNow();
        if (this.self != null) this.self.interrupt();
        
        if (this.shutdown_latch != null) {
            try {
                this.shutdown_latch.acquire();
            } catch (InterruptedException ex) {
                // Ignore
            } catch (Exception ex) {
                LOG.fatal("Unexpected error while shutting down", ex);
            }
        }
    }
}
