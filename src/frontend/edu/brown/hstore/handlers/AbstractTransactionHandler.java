package edu.brown.hstore.handlers;

import java.util.Collection;

import org.apache.log4j.Logger;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;

/**
 * AbstractTransactionHandler is a wrapper around the invocation methods for some action
 * There is a local and remote method to send a message to the necessary HStoreSites
 * for a transaction. The sendMessages() method is the main entry point that the 
 * HStoreCoordinator's convenience methods will use. 
 * @param <T> The message that we will send out on the network
 * @param <U> The expected message that we will need to get back as a response 
 */
public abstract class AbstractTransactionHandler<T extends GeneratedMessage, U extends GeneratedMessage> {
    private static final Logger LOG = Logger.getLogger(AbstractTransactionHandler.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected final HStoreSite hstore_site;
    protected final HStoreCoordinator hstore_coord;
    protected final HStoreService handler;
    protected final int num_sites;
    protected final int local_site_id;
    
    public AbstractTransactionHandler(HStoreSite hstore_site, HStoreCoordinator hstore_coord) {
        this.hstore_site = hstore_site;
        this.hstore_coord = hstore_coord;
        this.handler = this.hstore_coord.getHandler();
        this.num_sites = CatalogUtil.getNumberOfSites(hstore_site.getSite());
        this.local_site_id = hstore_site.getSiteId();
    }
    
    // (kowshik) Added extra method to send messages to replicas
    /****** FOR REPLICATION ****************/
    public void sendMessagesToReplicas(LocalTransaction ts, T request, RpcCallback<U> callback, int partitionId, Collection<Integer> replicaIds) {
    	for (Integer replicaId : replicaIds) {
    		HStoreService channel = hstore_coord.getChannel(replicaId);
            assert(channel != null) : "Invalid partition id '" + partitionId + "'";
            ProtoRpcController controller = this.getProtoRpcController(ts, replicaId);
            assert(controller != null) : "Invalid " + request.getClass().getSimpleName() + " ProtoRpcController for site #" + replicaId;
            this.sendRemote(channel, controller, request, callback);
            LOG.info(String.format("(kowshik/vijay) Sent request: %s to replica ID: %s for partition: %d for transaction: %s",
                    request, replicaId, partitionId, ts));
    	}
    }
    /***********************************/
    
    /**
     * Send a copy of a single message request to the partitions given as input
     * If a partition is managed by the local HStoreSite, then we will invoke
     * the sendLocal() method. If it is on a remote HStoreSite, then we will
     * invoke sendRemote().
     * @param ts
     * @param request
     * @param callback
     * @param partitions
     */
    public void sendMessages(LocalTransaction ts, T request, RpcCallback<U> callback, Collection<Integer> partitions) {
        // If this flag is true, then we'll invoke the local method
        // We want to do this *after* we send out all the messages to the remote sites
        // so that we don't have to wait as long for the responses to come back over the network
        boolean send_local = false;
        boolean site_sent[] = new boolean[this.num_sites];
        int ctr = 0;
        for (Integer p : partitions) {
            int dest_site_id = hstore_site.getSiteIdForPartitionId(p);

            // Skip this HStoreSite if we're already sent it a message 
            if (site_sent[dest_site_id]) continue;
            
            if (trace.get())
                LOG.trace(String.format("Sending %s message to %s for %s",
                                        request.getClass().getSimpleName(), HStoreSite.formatSiteName(dest_site_id), ts));
            
            // Local Partition
            if (this.local_site_id == dest_site_id) {
                send_local = true;
            // Remote Partition
            } else {
                HStoreService channel = hstore_coord.getChannel(dest_site_id);
                assert(channel != null) : "Invalid partition id '" + p + "'";
                ProtoRpcController controller = this.getProtoRpcController(ts, dest_site_id);
                assert(controller != null) : "Invalid " + request.getClass().getSimpleName() + " ProtoRpcController for site #" + dest_site_id;
                this.sendRemote(channel, controller, request, callback);
            }
            site_sent[dest_site_id] = true;
            ctr++;
        } // FOR
        // Optimization: We'll invoke sendLocal() after we have sent out
        // all of the mesages to remote sites
        if (send_local) this.sendLocal(ts.getTransactionId(), request, partitions, callback);
        
        if (debug.get())
            LOG.debug(String.format("Sent %d %s to %d partitions for %s",
                                    ctr, request.getClass().getSimpleName(),  partitions.size(), ts));
    }
    
    /**
     * The processing method that is invoked if the outgoing message needs
     * to be sent to a partition that is on the same machine as where this
     * handler is executing.
     * @param txn_id
     * @param request
     * @param partitions
     * @param callback
     */
    public abstract void sendLocal(long txn_id, T request, Collection<Integer> partitions, RpcCallback<U> callback);
    
    /**
     * The processing method that is invoked if the outgoing message needs
     * to be sent to a partition that is *not* managed by the same HStoreSite
     * as where this handler is executing. This is non-blocking and does not 
     * wait for the callback to be executed in response to the remote side.
     * @param channel
     * @param controller
     * @param request
     * @param callback
     */
    public abstract void sendRemote(HStoreService channel, ProtoRpcController controller, T request, RpcCallback<U> callback);
    
    /**
     * This is the method that is invoked on the remote HStoreSite for each incoming
     * message request. This will then determine whether the message should be queued up
     * for execution in a different thread, or whether it should invoke remoteHandler()
     * right away.
     * @param controller
     * @param request
     * @param callback
     */
    public abstract void remoteQueue(RpcController controller, T request, RpcCallback<U> callback);
    
    /**
     * The remoteHandler is the code that executes on the remote node to process
     * the request for a transaction that is executing on a different node.
     * @param controller
     * @param request
     * @param callback
     */
    public abstract void remoteHandler(RpcController controller, T request, RpcCallback<U> callback);
    
    /**
     * Return a cached ProtoRpcController handler from the LocalTransaction object
     * @param ts
     * @param site_id
     * @return
     */
    protected abstract ProtoRpcController getProtoRpcController(LocalTransaction ts, int site_id);
}