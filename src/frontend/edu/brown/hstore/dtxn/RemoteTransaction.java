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
package edu.brown.hstore.dtxn;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.callbacks.TransactionCleanupCallback;
import edu.brown.hstore.callbacks.TransactionWorkCallback;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.StringUtil;

/**
 * A RemoteTransaction is one whose Java control code is executing at a 
 * different partition then where we are using this handle.
 * @author pavlo
 */
public class RemoteTransaction extends AbstractTransaction {
    protected static final Logger LOG = Logger.getLogger(RemoteTransaction.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final TransactionWorkCallback fragment_callback;
    private final TransactionCleanupCallback cleanup_callback;
    
    public RemoteTransaction(HStoreSite hstore_site) {
        super(hstore_site);
        this.fragment_callback = new TransactionWorkCallback(hstore_site);
        this.cleanup_callback = new TransactionCleanupCallback(hstore_site);
    }
    
    public RemoteTransaction init(long txnId, int source_partition, boolean sysproc, boolean predict_abortable) {
        return ((RemoteTransaction)super.init(txnId, -1, source_partition, sysproc,
                                              false, true, predict_abortable, false));
    }
    
    @Override
    public void finish() {
        super.finish();
        this.cleanup_callback.finish();
    }
    
    @Override
    public void startRound(int partition) {
        // If the stored procedure is not executing locally then we need at least
        // one FragmentTaskMessage callback
        assert(this.fragment_callback != null) :
            "No FragmentTaskMessage callbacks available for txn #" + this.txn_id;
        super.startRound(partition);
    }
    
    /**
     * Return the previously stored callback for a FragmentTaskMessage
     * @return
     */
    public TransactionWorkCallback getFragmentTaskCallback() {
        return (this.fragment_callback);
    }
    
    public TransactionCleanupCallback getCleanupCallback() {
        return (this.cleanup_callback);
    }

    @Override
    public String toString() {
        if (this.isInitialized()) {
//            return "REMOTE #" + this.txn_id;
            return String.format("REMOTE #%d/%d", this.txn_id, this.base_partition);
        } else {
            return ("<Uninitialized>");
        }
    }
    
    @Override
    public String debug() {
        return (StringUtil.formatMaps(this.getDebugMap()));
    }
}

