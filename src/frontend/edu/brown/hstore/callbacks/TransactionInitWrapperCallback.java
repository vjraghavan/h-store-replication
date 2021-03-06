package edu.brown.hstore.callbacks;

import java.util.Collection;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * This callback is used to wrap around the network-outbound TransactionInitResponse callback on
 * an HStoreSite that is being asked to participate in a distributed transaction from another
 * HStoreSite. Only when we get all the acknowledgments (through the run method) for the local 
 * partitions at this HStoreSite will we invoke the original callback.
 * @author pavlo
 */
public class TransactionInitWrapperCallback extends BlockingCallback<TransactionInitResponse, Integer> {
    private static final Logger LOG = Logger.getLogger(TransactionInitWrapperCallback.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
            
    private TransactionInitResponse.Builder builder = null;
    private Collection<Integer> partitions = null;
    
    public TransactionInitWrapperCallback(HStoreSite hstore_site) {
        super(hstore_site, false);
    }
    
    public void init(long txn_id, Collection<Integer> partitions, RpcCallback<TransactionInitResponse> orig_callback) {
        if (debug.get())
            LOG.debug(String.format("Starting new %s for txn #%d", this.getClass().getSimpleName(), txn_id));
        assert(orig_callback != null) :
            String.format("Tried to initialize %s with a null callback for txn #%d", this.getClass().getSimpleName(), txn_id);
        assert(partitions != null) :
            String.format("Tried to initialize %s with a null partitions for txn #%d", this.getClass().getSimpleName(), txn_id);
        
        // Only include local partitions
        int counter = 0;
        Collection<Integer> localPartitions = hstore_site.getLocalPartitionIds();
        for (Integer p : partitions) {
            if (localPartitions.contains(p)) counter++;
        } // FOR
        assert(counter > 0);
        this.partitions = partitions;
        this.builder = TransactionInitResponse.newBuilder()
                             .setTransactionId(txn_id)
                             .setStatus(Hstoreservice.Status.OK);
        super.init(txn_id, counter, orig_callback);
    }
    
    public Collection<Integer> getPartitions() {
        return (this.partitions);
    }
    
    @Override
    protected void finishImpl() {
        this.builder = null;
    }
    
    @Override
    public boolean isInitialized() {
        return (this.builder != null && super.isInitialized());
    }
    
    @Override
    public void unblockCallback() {
        if (debug.get()) {
            LOG.debug(String.format("Txn #%d - Sending %s to %s with status %s",
                                    this.getTransactionId(),
                                    TransactionInitResponse.class.getSimpleName(),
                                    this.getOrigCallback().getClass().getSimpleName(),
                                    this.builder.getStatus()));
        }
        assert(this.getOrigCounter() == builder.getPartitionsCount()) :
            String.format("The %s for txn #%d has results from %d partitions but it was suppose to have %d.",
                          builder.getClass().getSimpleName(), this.getTransactionId(), builder.getPartitionsCount(), this.getOrigCounter());
        assert(this.getOrigCallback() != null) :
            String.format("The original callback for txn #%d is null!", this.getTransactionId());
        this.getOrigCallback().run(this.builder.build());
        
        // TODO(cjl6): At this point all of the partitions at this HStoreSite are allocated
        //             for executing this txn. We can now check whether it has any embedded
        //             queries that need to be queued up for pre-fetching. If so, blast them
        //             off to the HStoreSite so that they can be executed in the PartitionExecutor
        //             Use txn_id to get the AbstractTransaction handle from the HStoreSite 
    }
    
    public void abort(Hstoreservice.Status status, int partition, long txn_id) {
        this.builder.setRejectPartition(partition);
        this.builder.setRejectTransactionId(txn_id);
        this.abort(status);
    }
    
    @Override
    protected void abortCallback(Hstoreservice.Status status) {
        if (debug.get())
            LOG.debug(String.format("Txn #%d - Aborting %s with status %s",
                                    this.getTransactionId(), this.getClass().getSimpleName(), status));
        this.builder.setStatus(status);
        Collection<Integer> localPartitions = hstore_site.getLocalPartitionIds();
        for (Integer p : this.partitions) {
            if (localPartitions.contains(p) && this.builder.getPartitionsList().contains(p) == false) {
                this.builder.addPartitions(p.intValue());
            }
        } // FOR
        this.unblockCallback();
    }
    
    @Override
    protected synchronized int runImpl(Integer partition) {
        if (this.isAborted() == false)
            this.builder.addPartitions(partition.intValue());
        return 1;
    }
}
