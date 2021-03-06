package edu.brown.hstore.dispatchers;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.Hstoreservice.TransactionInitRequest;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;

public class TransactionInitDispatcher extends AbstractDispatcher<Object[]> {
    
    public TransactionInitDispatcher(HStoreCoordinator hStoreCoordinator) {
        super(hStoreCoordinator);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void runImpl(Object o[]) {
        RpcController controller = (RpcController)o[0];
        TransactionInitRequest request = (TransactionInitRequest)o[1];
        RpcCallback<TransactionInitResponse> callback = (RpcCallback<TransactionInitResponse>)o[2];
        hStoreCoordinator.getTransactionInitHandler().remoteHandler(controller, request, callback);
    }

}
