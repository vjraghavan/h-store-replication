package edu.brown.hstore;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;

import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.LocalTransaction;

public class MockHStoreSite extends HStoreSite {
    private static final Logger LOG = Logger.getLogger(MockHStoreSite.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------------------
    // STATIC HELPERS
    // ----------------------------------------------------------------------------
    
    static LocalTransaction makeLocalTransaction(HStoreSite hstore_site) {
        long txnId = hstore_site.getTransactionIdManager().getNextUniqueTransactionId();
        long clientHandle = -1;
        int base_partition = CollectionUtil.random(hstore_site.getLocalPartitionIds());
        Collection<Integer> predict_touchedPartitions = hstore_site.getAllPartitionIds();
        boolean predict_readOnly = false;
        boolean predict_canAbort = true;
        Procedure catalog_proc = hstore_site.getDatabase().getProcedures().getIgnoreCase("@NoOp");
        StoredProcedureInvocation invocation = new StoredProcedureInvocation(clientHandle, catalog_proc.getName());
        RpcCallback<byte[]> client_callback = null;
        
        LocalTransaction ts = new LocalTransaction(hstore_site);
        ts.init(txnId, clientHandle, base_partition,
                predict_touchedPartitions, predict_readOnly, predict_canAbort,
                catalog_proc, invocation, client_callback);
        return (ts);
    }
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    public MockHStoreSite(Site catalog_site, HStoreConf hstore_conf) {
        super(catalog_site, hstore_conf);
        
        for (Integer p : CatalogUtil.getLocalPartitionIds(catalog_site)) {
            this.addPartitionExecutor(p, new MockPartitionExecutor(p, catalog_site.getCatalog(), this.getPartitionEstimator()));
        }
    }
    @Override
    public HStoreCoordinator initHStoreCoordinator() {
        return new MockHStoreCoordinator(this);
    }
    
    // ----------------------------------------------------------------------------
    // SPECIAL METHOD OVERRIDES
    // ----------------------------------------------------------------------------
    
    @Override
    public void transactionRestart(LocalTransaction orig_ts, Status status) {
        int restart_limit = 10;
        if (orig_ts.getRestartCounter() > restart_limit) {
            if (orig_ts.isSysProc()) {
                throw new RuntimeException(String.format("%s has been restarted %d times! Rejecting...",
                                           orig_ts, orig_ts.getRestartCounter()));
            } else {
                this.transactionReject(orig_ts, Status.ABORT_REJECT);
                return;
            }
        }
    }

    // ----------------------------------------------------------------------------
    // YE OLD MAIN METHOD
    // ----------------------------------------------------------------------------
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs,
            ArgumentsParser.PARAM_CATALOG
        );
        int site_id = args.getIntOptParam(0);
        
        Site catalog_site = CatalogUtil.getSiteFromId(args.catalog, site_id);
        assert(catalog_site != null) : "Invalid site id #" + site_id;
        
        HStoreConf hstore_conf = HStoreConf.initArgumentsParser(args, catalog_site);
        hstore_conf.site.cpu_affinity = false;
        hstore_conf.site.status_enable = false;
        
        final MockHStoreSite hstore_site = new MockHStoreSite(catalog_site, hstore_conf);
        hstore_site.init().start(); // Blocks until all connections are established
        final MockHStoreCoordinator hstore_coordinator = (MockHStoreCoordinator)hstore_site.getCoordinator();
        assert(hstore_coordinator.isStarted());
        
        final CountDownLatch latch = new CountDownLatch(1);
        
        // Let's try one!
        if (site_id == 0) {
            // Sleep for a few seconds give the other guys time to prepare themselves
            ThreadUtil.sleep(2500);
            
            final LocalTransaction ts = makeLocalTransaction(hstore_site);
            RpcCallback<TransactionInitResponse> callback = new RpcCallback<TransactionInitResponse>() {
                @Override
                public void run(TransactionInitResponse parameter) {
                    LOG.info("GOT CALLBACK FOR " + ts);
                    latch.countDown();
                }
            };
            LOG.info("Sending init for " + ts);
            hstore_coordinator.transactionInit(ts, callback);
        }
        
        // Block until we get our response!
        latch.await();
    }
}
