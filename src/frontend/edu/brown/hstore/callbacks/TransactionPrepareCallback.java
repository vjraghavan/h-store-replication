package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionPrepareResponse;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * 
 * @author pavlo
 */
public class TransactionPrepareCallback extends BlockingCallback<byte[], TransactionPrepareResponse> {
    private static final Logger LOG = Logger.getLogger(TransactionPrepareCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final boolean txn_profiling;
    private LocalTransaction ts;
    private ClientResponseImpl cresponse;

    /**
     * Constructor
     * @param hstore_site
     */
    public TransactionPrepareCallback(HStoreSite hstore_site) {
        super(hstore_site, false);
        this.txn_profiling = hstore_site.getHStoreConf().site.txn_profiling;
    }
    
    public void init(LocalTransaction ts) {
        this.ts = ts;
        super.init(ts.getTransactionId(), ts.getPredictTouchedPartitions().size(), ts.getClientCallback());
    }
    
    public void setClientResponse(ClientResponseImpl cresponse) {
        assert(this.cresponse == null);
        this.cresponse = cresponse;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.ts != null && super.isInitialized());
    }
    
    @Override
    public void finishImpl() {
        this.ts = null;
        this.cresponse = null;
    }
    
    @Override
    public void unblockCallback() {
        assert(this.cresponse != null) : "Trying to send back ClientResponse for " + ts + " before it was set!";
        
        // At this point all of our HStoreSites came back with an OK on the 2PC PREPARE
        // So that means we can send back the result to the client and then 
        // send the 2PC COMMIT message to all of our friends.
        // We want to do this first because the transaction state could get
        // cleaned-up right away when we call HStoreCoordinator.transactionFinish()
        this.hstore_site.sendClientResponse(this.ts, this.cresponse);
        
        // Everybody returned ok, so we'll tell them all commit right now
        this._finish(Status.OK);
    }
    
    @Override
    protected void abortCallback(Status status) {
        // As soon as we get an ABORT from any partition, fire off the final ABORT 
        // to all of the partitions
        this._finish(status);
        
        // Change the response's status and send back the result to the client
        this.cresponse.setStatus(status);
        this.hstore_site.sendClientResponse(this.ts, this.cresponse);
    }
    
    private void _finish(Status status) {
        if (this.txn_profiling) {
            this.ts.profiler.stopPostPrepare();
            this.ts.profiler.startPostFinish();
        }
        
        // Let everybody know that the party is over!
        TransactionFinishCallback finish_callback = this.ts.initTransactionFinishCallback(status);
        this.hstore_site.getCoordinator().transactionFinish(this.ts, status, finish_callback);
    }
    
    @Override
    protected int runImpl(TransactionPrepareResponse response) {
        if (debug.get())
            LOG.debug(String.format("Got %s with %d partitions for %s",
                                    response.getClass().getSimpleName(),
                                    response.getPartitionsCount(),
                                    this.ts));
        assert(this.ts.getTransactionId() == response.getTransactionId()) :
            String.format("Unexpected %s for a different transaction %s != #%d",
                          response.getClass().getSimpleName(), this.ts, response.getTransactionId());
        final Hstoreservice.Status status = response.getStatus();
        
        // If any TransactionPrepareResponse comes back with anything but an OK,
        // then the we need to abort the transaction immediately
        if (status != Hstoreservice.Status.OK) {
            this.abort(status);
        }

        // Otherwise we need to update our counter to keep track of how many OKs that we got
        // back. We'll ignore anything that comes in after we've aborted
        return response.getPartitionsCount();
    }
} // END CLASS