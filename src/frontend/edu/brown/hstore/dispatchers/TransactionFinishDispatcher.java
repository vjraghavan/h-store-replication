package edu.brown.hstore.dispatchers;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.Hstoreservice.TransactionFinishRequest;
import edu.brown.hstore.Hstoreservice.TransactionFinishResponse;

public class TransactionFinishDispatcher extends AbstractDispatcher<Object[]> {
    
    public TransactionFinishDispatcher(HStoreCoordinator hStoreCoordinator) {
        super(hStoreCoordinator);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void runImpl(Object o[]) {
        RpcController controller = (RpcController)o[0];
        TransactionFinishRequest request = (TransactionFinishRequest)o[1];
        RpcCallback<TransactionFinishResponse> callback = (RpcCallback<TransactionFinishResponse>)o[2];
        hStoreCoordinator.getTransactionFinishHandler().remoteHandler(controller, request, callback);
    }

}
