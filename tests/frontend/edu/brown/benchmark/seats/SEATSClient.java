/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
/* This file is part of VoltDB. 
 * Copyright (C) 2009 Vertica Systems Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR 
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.                       
 */

package edu.brown.benchmark.seats;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;

import edu.brown.benchmark.seats.SEATSConstants.ErrorType;
import edu.brown.benchmark.seats.procedures.DeleteReservation;
import edu.brown.benchmark.seats.procedures.FindFlights;
import edu.brown.benchmark.seats.procedures.FindOpenSeats;
import edu.brown.benchmark.seats.procedures.NewReservation;
import edu.brown.benchmark.seats.procedures.UpdateCustomer;
import edu.brown.benchmark.seats.procedures.UpdateReservation;
import edu.brown.benchmark.seats.util.CustomerId;
import edu.brown.benchmark.seats.util.FlightId;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.Histogram;
import edu.brown.utils.StringUtil;

public class SEATSClient extends SEATSBaseClient {
    private static final Logger LOG = Logger.getLogger(SEATSClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * Airline Benchmark Transactions
     */
    public static enum Transaction {
        DELETE_RESERVATION          (DeleteReservation.class,   SEATSConstants.FREQUENCY_DELETE_RESERVATION),
        FIND_FLIGHTS                (FindFlights.class,         SEATSConstants.FREQUENCY_FIND_FLIGHTS),
        FIND_OPEN_SEATS             (FindOpenSeats.class,       SEATSConstants.FREQUENCY_FIND_OPEN_SEATS),
        NEW_RESERVATION             (NewReservation.class,      SEATSConstants.FREQUENCY_NEW_RESERVATION),
        UPDATE_CUSTOMER             (UpdateCustomer.class,      SEATSConstants.FREQUENCY_UPDATE_CUSTOMER),
        UPDATE_RESERVATION          (UpdateReservation.class,   SEATSConstants.FREQUENCY_UPDATE_RESERVATION);
        
        private Transaction(Class<? extends VoltProcedure> proc_class, int weight) {
            this.proc_class = proc_class;
            this.execName = proc_class.getSimpleName();
            this.default_weight = weight;
            this.displayName = StringUtil.title(this.name().replace("_", " "));
        }

        public final Class<? extends VoltProcedure> proc_class;
        public final int default_weight;
        public final String displayName;
        public final String execName;
        
        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<Integer, Transaction>();
        protected static final Map<String, Transaction> name_lookup = new HashMap<String, Transaction>();
        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
                Transaction.name_lookup.put(vt.name().toLowerCase().intern(), vt);
            }
        }
        
        public static Transaction get(Integer idx) {
            assert(idx >= 0);
            return (Transaction.idx_lookup.get(idx));
        }

        public static Transaction get(String name) {
            return (Transaction.name_lookup.get(name.toLowerCase().intern()));
        }
        public int getDefaultWeight() {
            return (this.default_weight);
        }
        public String getDisplayName() {
            return (this.displayName);
        }
        public String getExecName() {
            return (this.execName);
        }
    }
    
    // -----------------------------------------------------------------
    // SEPARATE CALLBACK PROCESSING THREAD
    // -----------------------------------------------------------------
    
    protected abstract class AbstractCallback<T> implements ProcedureCallback {
        final Transaction txn;
        final T element;
        public AbstractCallback(Transaction txn, T t) {
            this.txn = txn;
            this.element = t;
        }
        public final void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, txn.ordinal());
            callbackQueue.add(new Pair<AbstractCallback<?>, ClientResponse>(this, clientResponse));
        }
        public abstract void clientCallbackImpl(ClientResponse clientResponse);
    }
    
    private static Thread callbackThread;
    private static final LinkedBlockingQueue<Pair<AbstractCallback<?>, ClientResponse>> callbackQueue = new LinkedBlockingQueue<Pair<AbstractCallback<?>,ClientResponse>>();

    
    // -----------------------------------------------------------------
    // RESERVED SEAT BITMAPS
    // -----------------------------------------------------------------
    
    public enum CacheType {
        PENDING_INSERTS     (SEATSConstants.CACHE_LIMIT_PENDING_INSERTS),
        PENDING_UPDATES     (SEATSConstants.CACHE_LIMIT_PENDING_UPDATES),
        PENDING_DELETES     (SEATSConstants.CACHE_LIMIT_PENDING_DELETES),
        ;
        
        private CacheType(int limit) {
            this.limit = limit;
            this.lock = new ReentrantLock();
        }
        
        private final int limit;
        private final ReentrantLock lock;
    }
    
    protected final Map<CacheType, LinkedList<Reservation>> CACHE_RESERVATIONS = new HashMap<SEATSClient.CacheType, LinkedList<Reservation>>();
    {
        for (CacheType ctype : CacheType.values()) {
            CACHE_RESERVATIONS.put(ctype, new LinkedList<Reservation>());
        } // FOR
    } // STATIC 
    
    
    protected static final ConcurrentHashMap<CustomerId, Set<FlightId>> CACHE_CUSTOMER_BOOKED_FLIGHTS = new ConcurrentHashMap<CustomerId, Set<FlightId>>();
    protected static final Map<FlightId, BitSet> CACHE_BOOKED_SEATS = new HashMap<FlightId, BitSet>();

    
    private static final BitSet FULL_FLIGHT_BITSET = new BitSet(SEATSConstants.NUM_SEATS_PER_FLIGHT);
    static {
        for (int i = 0; i < SEATSConstants.NUM_SEATS_PER_FLIGHT; i++)
            FULL_FLIGHT_BITSET.set(i);
    } // STATIC
    
    protected static BitSet getSeatsBitSet(FlightId flight_id) {
        BitSet seats = CACHE_BOOKED_SEATS.get(flight_id);
        if (seats == null) {
            synchronized (CACHE_BOOKED_SEATS) {
                seats = CACHE_BOOKED_SEATS.get(flight_id);
                if (seats == null) {
                    seats = new BitSet(SEATSConstants.NUM_SEATS_PER_FLIGHT);
                    CACHE_BOOKED_SEATS.put(flight_id, seats);
                }
            } // SYNCH
        }
        return (seats);
    }
    
    /**
     * Returns true if the given BitSet for a Flight has all of its seats reserved 
     * @param seats
     * @return
     */
    protected static boolean isFlightFull(BitSet seats) {
        assert(FULL_FLIGHT_BITSET.size() == seats.size());
        return FULL_FLIGHT_BITSET.equals(seats);
    }
    
    /**
     * Returns true if the given Customer already has a reservation booked on the target Flight
     * @param customer_id
     * @param flight_id
     * @return
     */
    protected boolean isCustomerBookedOnFlight(CustomerId customer_id, FlightId flight_id) {
        Set<FlightId> flights = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
        return (flights != null && flights.contains(flight_id));
    }

    /**
     * Returns the set of Customers that are waiting to be added the given Flight
     * @param flight_id
     * @return
     */
    protected Set<CustomerId> getPendingCustomers(FlightId flight_id) {
        Set<CustomerId> customers = new HashSet<CustomerId>();
        CacheType.PENDING_INSERTS.lock.lock();
        try {
            for (Reservation r : CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS)) {
                if (r.flight_id.equals(flight_id)) customers.add(r.customer_id);
            } // FOR
        } finally {
            CacheType.PENDING_INSERTS.lock.unlock();
        } // SYNCH
        return (customers);
    }
    
    /**
     * Returns true if the given Customer is pending to be booked on the given Flight
     * @param customer_id
     * @param flight_id
     * @return
     */
    protected boolean isCustomerPendingOnFlight(CustomerId customer_id, FlightId flight_id) {
        CacheType.PENDING_INSERTS.lock.lock();
        try {
            for (Reservation r : CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS)) {
                if (r.flight_id.equals(flight_id) && r.customer_id.equals(customer_id)) {
                    return (true);
                }
            } // FOR
        } finally {
            CacheType.PENDING_INSERTS.lock.unlock();
        } // SYNCH
        return (false);
    }
    
    protected Set<FlightId> getCustomerBookedFlights(CustomerId customer_id) {
        Set<FlightId> f_ids = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
        if (f_ids == null) {
            synchronized (CACHE_CUSTOMER_BOOKED_FLIGHTS) {
                f_ids = CACHE_CUSTOMER_BOOKED_FLIGHTS.get(customer_id);
                if (f_ids == null) {
                    f_ids = new HashSet<FlightId>();
                    CACHE_CUSTOMER_BOOKED_FLIGHTS.put(customer_id, f_ids);
                }
            } // SYNCH
        }
        return (f_ids);
    }
    
    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (CacheType ctype : CACHE_RESERVATIONS.keySet()) {
            m.put(ctype.name(), CACHE_RESERVATIONS.get(ctype));
        } // FOR
        m.put("CACHE_CUSTOMER_BOOKED_FLIGHTS", CACHE_CUSTOMER_BOOKED_FLIGHTS); 
        m.put("CACHE_BOOKED_SEATS", CACHE_BOOKED_SEATS); 
        
        return StringUtil.formatMaps(m);
    }
    
    // -----------------------------------------------------------------
    // ADDITIONAL DATA MEMBERS
    // -----------------------------------------------------------------
    
    private final RandomDistribution.FlatHistogram<Transaction> xacts;
    
    /**
     * When a customer looks for an open seat, they will then attempt to book that seat in
     * a new reservation. Some of them will want to change their seats. This data structure
     * represents a customer that is queued to change their seat. 
     */
    protected static class Reservation {
        public final long id;
        public final FlightId flight_id;
        public final CustomerId customer_id;
        public final int seatnum;
        
        public Reservation(long id, FlightId flight_id, CustomerId customer_id, int seatnum) {
            this.id = id;
            this.flight_id = flight_id;
            this.customer_id = customer_id;
            this.seatnum = seatnum;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Reservation) {
                Reservation r = (Reservation)obj;
                // Ignore id!
                return (this.seatnum == r.seatnum &&
                        this.flight_id.equals(r.flight_id) &&
                        this.customer_id.equals(r.customer_id));
                        
            }
            return (false);
        }
        
        @Override
        public String toString() {
            return String.format("{Id:%d / %s / %s / SeatNum:%d}",
                                 this.id, this.flight_id, this.customer_id, this.seatnum);
        }
    } // END CLASS

    // -----------------------------------------------------------------
    // REQUIRED METHODS
    // -----------------------------------------------------------------

    public static void main(String args[]) {
        edu.brown.benchmark.BenchmarkComponent.main(SEATSClient.class, args, false);
    }

    public SEATSClient(String[] args) {
        super(args);

        // Initialize Default Weights
        final Histogram<Transaction> weights = new Histogram<Transaction>();
        for (Transaction t : Transaction.values()) {
            weights.put(t, t.getDefaultWeight());
        } // FOR

        // Process additional parameters
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);
            
            // Transaction Weights
            // Expected format: -Dxactweight=TRANSACTION_NAME:###
            if (key.equalsIgnoreCase("xactweight")) {
                String parts[] = value.split(":");
                Transaction t = Transaction.get(parts[0]);
                assert(t == null) : "Invalid transaction name '" + parts[0] + "'";
                Integer weight = Integer.parseInt(parts[1]);
                assert(weight == null) : "Invalid weight '" + parts[1] + "' for transaction " + t;
                weights.set(t, weight);
            }
        } // FOR
        
        if (this.noClientConnections() == false) {
            this.profile.loadProfile(this);
            if (trace.get()) LOG.trace("Airport Max Customer Id:\n" + this.profile.airport_max_customer_id);
            
            // Make sure we have the information we need in the BenchmarkProfile
            String error_msg = null;
            if (this.getFlightIdCount() == 0) {
                error_msg = "The benchmark profile does not have any flight ids.";
            } else if (this.getCustomerIdCount() == 0) {
                error_msg = "The benchmark profile does not have any customer ids.";
            } else if (this.getFlightStartDate() == null) {
                error_msg = "The benchmark profile does not have a valid flight start date.";
            }
            if (error_msg != null) throw new RuntimeException(error_msg);
        }
        
        // Create xact lookup array
        this.xacts = new RandomDistribution.FlatHistogram<Transaction>(rng, weights);
        assert(weights.getSampleCount() == 100) : "The total weight for the transactions is " + this.xacts.getSampleCount() + ". It needs to be 100";
        if (debug.get()) LOG.debug("Transaction Execution Distribution:\n" + weights);
        
        synchronized (SEATSClient.class) {
            if (callbackThread == null) {
                callbackThread = new Thread(new CallbackProcessor());
                callbackThread.setDaemon(true);
                callbackThread.start();
            }
        } // SYNCH
    }

    @Override
    public String[] getTransactionDisplayNames() {
        String names[] = new String[Transaction.values().length];
        int ii = 0;
        for (Transaction t : Transaction.values()) {
            names[ii++] = t.getDisplayName();
        }
        return names;
    }

    @Override
    public void runLoop() {
        final Client client = this.getClientHandle();

        // Fire off a FindOpenSeats so that we can prime ourselves
        try {
            boolean ret = this.executeFindOpenSeats(Transaction.FIND_OPEN_SEATS);
            assert(ret);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        
        // Execute Transactions
        try {
            while (true) {
                runOnce();
                client.backpressureBarrier();
            } // WHILE
        } catch (InterruptedException e) {
            e.printStackTrace();
            return;
        } catch (NoConnectionsException e) {
            /*
             * Client has no clean mechanism for terminating with the DB.
             */
            return;
        } catch (IOException e) {
            /*
             * At shutdown an IOException is thrown for every connection to
             * the DB that is lost Ignore the exception here in order to not
             * get spammed, but will miss lost connections at runtime
             */
        }
    }
    
    private AtomicBoolean first = new AtomicBoolean(true);
    

    @Override
    protected boolean runOnce() throws IOException {
        if (this.first.compareAndSet(true, false)) {
            // Fire off a FindOpenSeats so that we can prime ourselves
            try {
                boolean ret = this.executeFindOpenSeats(Transaction.FIND_OPEN_SEATS);
                assert(ret);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        
        int tries = 10;
        boolean ret = false;
        while (tries-- > 0 && ret == false) {
            Transaction txn = this.xacts.nextValue();
            if (debug.get()) LOG.debug("Attempting to execute " + txn);
            switch (txn) {
                case DELETE_RESERVATION: {
                    ret = this.executeDeleteReservation(txn);
                    break;
                }
                case FIND_FLIGHTS: {
                    ret = this.executeFindFlights(txn);
                    break;
                }
                case FIND_OPEN_SEATS: {
                    ret = this.executeFindOpenSeats(txn);
                    break;
                }
                case NEW_RESERVATION: {
                    ret = this.executeNewReservation(txn);
                    break;
                }
                case UPDATE_CUSTOMER: {
                    ret = this.executeUpdateCustomer(txn);
                    break;
                }
                case UPDATE_RESERVATION: {
                    ret = this.executeUpdateReservation(txn);
                    break;
                }
                default:
                    assert(false) : "Unexpected transaction: " + txn; 
            } // SWITCH
            if (ret && debug.get()) LOG.debug("Executed a new invocation of " + txn);
        }
        if (tries == 0) LOG.warn("I have nothing to do!");
        return (tries > 0);
    }
    
    @Override
    public void tickCallback(int counter) {
        super.tickCallback(counter);
        for (CacheType ctype : CACHE_RESERVATIONS.keySet()) {
            ctype.lock.lock();
            try {
                List<Reservation> cache = CACHE_RESERVATIONS.get(ctype);
                int before = cache.size();
                if (before > ctype.limit) {
                    Collections.shuffle(cache, rng);
                    while (cache.size() > ctype.limit) {
                        cache.remove(0);
                    } // WHILE
                    if (debug.get()) LOG.debug(String.format("Pruned records from cache [newSize=%d, origSize=%d]",
                                               cache.size(), before));
                } // IF
            } finally {
                ctype.lock.unlock();
            } // SYNCH
        } // FOR
        
        if (this.getClientId() == 0) {
            LOG.info("NewReservation Errors:\n" + newReservationErrors);
            newReservationErrors.clear();
        }
    }
    
    /**
     * Take an existing Reservation that we know is legit and randomly decide to 
     * either queue it for a later update or delete transaction 
     * @param r
     */
    protected void requeueReservation(Reservation r) {
        int val = rng.nextInt(100);
        
        // Queue this motha trucka up for a deletin'
        if (val < SEATSConstants.PROB_DELETE_NEW_RESERVATION) {
            CacheType.PENDING_DELETES.lock.lock();
            try {
                CACHE_RESERVATIONS.get(CacheType.PENDING_DELETES).add(r);
            } finally {
                CacheType.PENDING_DELETES.lock.unlock();
            } // SYNCH
        }
        // Or queue it for an update
        else if (val < SEATSConstants.PROB_UPDATE_NEW_RESERVATION + SEATSConstants.PROB_DELETE_NEW_RESERVATION) {
            CacheType.PENDING_UPDATES.lock.lock();
            try {
                CACHE_RESERVATIONS.get(CacheType.PENDING_UPDATES).add(r);
            } finally {
                CacheType.PENDING_UPDATES.lock.unlock();
            } // SYNCH
        }
    }
    
    
    protected static class CallbackProcessor implements Runnable {
        
        @Override
        public void run() {
            Pair<AbstractCallback<?>, ClientResponse> p = null;
            while (true) {
                try {
                    p = callbackQueue.take();
                } catch (InterruptedException ex) {
                    break;
                }
                if (trace.get()) LOG.trace("CallbackProcessor -> " + p.getFirst().getClass().getSimpleName());
                p.getFirst().clientCallbackImpl(p.getSecond());
            } // WHILE
        }
    }
    
    
    // -----------------------------------------------------------------
    // DeleteReservation
    // -----------------------------------------------------------------
    
    class DeleteReservationCallback extends AbstractCallback<Reservation> {
        public DeleteReservationCallback(Reservation r) {
            super(Transaction.DELETE_RESERVATION, r);
        }
        @Override
        public void clientCallbackImpl(ClientResponse clientResponse) {
            if (clientResponse.getStatus() == Status.OK) {
                // We can remove this from our set of full flights because know that there is now a free seat
                BitSet seats = SEATSClient.getSeatsBitSet(element.flight_id);
                seats.set(element.seatnum, false);
                
                // And then put it up for a pending insert
                if (rng.nextInt(100) < SEATSConstants.PROB_REQUEUE_DELETED_RESERVATION) {
                    CacheType.PENDING_INSERTS.lock.lock();
                    try {
                        CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS).add(element);
                    } finally {
                        CacheType.PENDING_INSERTS.lock.unlock();
                    } // SYNCH
                }
                
            } else if (debug.get()) {
                LOG.info("DeleteReservation " + clientResponse.getStatus() + ": " + clientResponse.getStatusString(), clientResponse.getException());
                LOG.info("BUSTED ID: " + element.flight_id + " / " + element.flight_id.encode());
            }
        }
    }

    private boolean executeDeleteReservation(Transaction txn) throws IOException {
        this.startComputeTime(txn.displayName);
        
        // Pull off the first cached reservation and drop it on the cluster...
        Reservation r = CACHE_RESERVATIONS.get(CacheType.PENDING_DELETES).poll();
        if (r == null) {
            this.stopComputeTime(txn.displayName);
            return (false);
        }
        int rand = rng.number(1, 100);
        
        Object params[] = new Object[]{
            r.flight_id.encode(),       // [0] f_id
            VoltType.NULL_BIGINT,       // [1] c_id
            "",                         // [2] c_id_str
            "",                         // [3] ff_c_id_str
            VoltType.NULL_BIGINT,       // [4] ff_al_id
        };
        
        // Delete with the Customer's id as a string 
        if (rand <= SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR) {
            params[2] = Long.toString(r.customer_id.encode());
        }
        // Delete using their FrequentFlyer information
        else if (rand <= SEATSConstants.PROB_DELETE_WITH_CUSTOMER_ID_STR + SEATSConstants.PROB_DELETE_WITH_FREQUENTFLYER_ID_STR) {
            params[3] = Long.toString(r.customer_id.encode());
            params[4] = r.flight_id.getSEATSId();
        }
        // Delete using their Customer id
        else {
            params[1] = r.customer_id.encode();
        }
        
        if (trace.get()) LOG.trace("Calling " + txn.getExecName());
        this.stopComputeTime(txn.displayName);
        this.getClientHandle().callProcedure(new DeleteReservationCallback(r),
                                             txn.getExecName(),
                                             params);
        return (true);
    }
    
    // ----------------------------------------------------------------
    // FindFlights
    // ----------------------------------------------------------------
    
    class FindFlightsCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, Transaction.FIND_FLIGHTS.ordinal());
            VoltTable[] results = clientResponse.getResults();
            if (results.length > 1) {
                // Convert the data into a FlightIds that other transactions can use
                int ctr = 0;
                while (results[0].advanceRow()) {
                    FlightId flight_id = new FlightId(results[0].getLong(0));
                    assert(flight_id != null);
                    boolean added = SEATSClient.this.addFlightId(flight_id);
                    if (added) ctr++;
                } // WHILE
                if (debug.get()) LOG.debug(String.format("Added %d out of %d FlightIds to local cache",
                                           ctr, results[0].getRowCount()));
            }
        }
    }

    /**
     * Execute one of the FindFlight transactions
     * @param txn
     * @throws IOException
     */
    private boolean executeFindFlights(Transaction txn) throws IOException {
        this.startComputeTime(txn.displayName);
        
        long depart_airport_id;
        long arrive_airport_id;
        TimestampType start_date;
        TimestampType stop_date;
        
        // Select two random airport ids
        if (rng.nextInt(100) < SEATSConstants.PROB_FIND_FLIGHTS_RANDOM_AIRPORTS) {
            // Does it matter whether the one airport actually flies to the other one?
            depart_airport_id = this.getRandomAirportId();
            arrive_airport_id = this.getRandomOtherAirport(depart_airport_id);
            
            // Select a random date from our upcoming dates
            start_date = this.getRandomUpcomingDate();
            stop_date = new TimestampType(start_date.getTime() + (SEATSConstants.MICROSECONDS_PER_DAY * 2));
        }
        
        // Use an existing flight so that we guaranteed to get back results
        else {
            FlightId flight_id = this.getRandomFlightId();
            depart_airport_id = flight_id.getDepartAirportId();
            arrive_airport_id = flight_id.getArriveAirportId();
            
            TimestampType flightDate = flight_id.getDepartDate(this.getFlightStartDate());
            long range = Math.round(SEATSConstants.MICROSECONDS_PER_DAY * 0.5);
            start_date = new TimestampType(flightDate.getTime() - range);
            stop_date = new TimestampType(flightDate.getTime() + range);
            
            if (debug.get())
                LOG.debug(String.format("Using %s as look up in %s: %d / %s",
                                        flight_id, txn, flight_id.encode(), flightDate));
        }
        
        // If distance is greater than zero, then we will also get flights from nearby airports
        long distance = -1;
        if (rng.nextInt(100) < SEATSConstants.PROB_FIND_FLIGHTS_NEARBY_AIRPORT) {
            distance = SEATSConstants.DISTANCES[rng.nextInt(SEATSConstants.DISTANCES.length)];
        }
        
        Object params[] = new Object[] {
            depart_airport_id,
            arrive_airport_id,
            start_date,
            stop_date,
            distance
        };
        if (trace.get()) LOG.trace("Calling " + txn.getExecName());
        this.stopComputeTime(txn.displayName);
        this.getClientHandle().callProcedure(new FindFlightsCallback(),
                                             txn.getExecName(),
                                             params);
        
        return (true);
    }

    // ----------------------------------------------------------------
    // FindOpenSeats
    // ----------------------------------------------------------------
    
    class FindOpenSeatsCallback extends AbstractCallback<FlightId> {
        public FindOpenSeatsCallback(FlightId f) {
            super(Transaction.FIND_OPEN_SEATS, f);
        }
        @Override
        public void clientCallbackImpl(ClientResponse clientResponse) {
            VoltTable[] results = clientResponse.getResults();
            if (results.length != 1) {
                if (debug.get()) LOG.warn("Results is " + results.length);
                return;
            }
            int rowCount = results[0].getRowCount();
            assert (rowCount <= SEATSConstants.NUM_SEATS_PER_FLIGHT) :
                String.format("Unexpected %d open seats returned for %s", rowCount, element);

            // there is some tiny probability of an empty flight .. maybe 1/(20**150)
            // if you hit this assert (with valid code), play the lottery!
            if (rowCount == 0) return;
            
            // Store pending reservations in our queue for a later transaction            
            List<Reservation> reservations = new ArrayList<Reservation>();
            Set<Integer> emptySeats = new HashSet<Integer>();
            Set<CustomerId> pendingCustomers = getPendingCustomers(element);
            while (results[0].advanceRow()) {
                FlightId flight_id = new FlightId(results[0].getLong(0));
                assert(flight_id.equals(element));
                int seatnum = (int)results[0].getLong(1);
                long airport_depart_id = flight_id.getDepartAirportId();
                
                // We first try to get a CustomerId based at this departure airport
                CustomerId customer_id = SEATSClient.this.getRandomCustomerId(airport_depart_id);
                
                // We will go for a random one if:
                //  (1) The Customer is already booked on this Flight
                //  (2) We already made a new Reservation just now for this Customer
                int tries = SEATSConstants.NUM_SEATS_PER_FLIGHT;
                while (tries-- > 0 && (customer_id == null || pendingCustomers.contains(customer_id) || isCustomerBookedOnFlight(customer_id, flight_id))) {
                    customer_id = SEATSClient.this.getRandomCustomerId();
                    if (trace.get()) LOG.trace("RANDOM CUSTOMER: " + customer_id);
                } // WHILE
                assert(customer_id != null) :
                    String.format("Failed to find a unique Customer to reserve for seat #%d on %s", seatnum, flight_id);

                pendingCustomers.add(customer_id);
                emptySeats.add(seatnum);
                reservations.add(new Reservation(getNextReservationId(), flight_id, customer_id, (int)seatnum));
                if (trace.get()) LOG.trace("QUEUED INSERT: " + flight_id + " / " + flight_id.encode() + " -> " + customer_id);
            } // WHILE
            
            if (reservations.isEmpty() == false) {
                int ctr = 0;
                Collections.shuffle(reservations);
                List<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
                assert(cache != null) : "Unexpected " + CacheType.PENDING_INSERTS;
                CacheType.PENDING_INSERTS.lock.lock();
                try {
                    for (Reservation r : reservations) {
                        if (cache.contains(r) == false) {
                            cache.add(r);
                            ctr++;
                        }
                    } // FOR
                } finally {
                    CacheType.PENDING_INSERTS.lock.unlock();
                } // SYNCH
                if (debug.get())
                    LOG.debug(String.format("Stored %d pending inserts for %s [totalPendingInserts=%d]",
                              ctr, element, cache.size()));
            }
            BitSet seats = getSeatsBitSet(element);
            for (int i = 0; i < SEATSConstants.NUM_SEATS_PER_FLIGHT; i++) {
                if (emptySeats.contains(i) == false) {
                    seats.set(i);
                }
            } // FOR
        }
    }

    /**
     * Execute the FindOpenSeat procedure
     * @throws IOException
     */
    private boolean executeFindOpenSeats(Transaction txn) throws IOException {
        this.startComputeTime(txn.displayName);
        FlightId flight_id = this.getRandomFlightId();
        assert(flight_id != null);
        
        Object params[] = new Object[] {
            flight_id.encode()
        };
        if (trace.get()) LOG.trace("Calling " + txn.getExecName());
        this.stopComputeTime(txn.displayName);
        
//        try {
//            this.getClientHandle().callProcedure(txn.execName, params);
//        } catch (ProcCallException ex) {
//            // throw new RuntimeException(ex);
//        }
        this.getClientHandle().callProcedure(new FindOpenSeatsCallback(flight_id),
                                             txn.execName,
                                             params);
        return (true);
    }
    
    // ----------------------------------------------------------------
    // NewReservation
    // ----------------------------------------------------------------
    
    private static final Histogram<String> newReservationErrors = new Histogram<String>();
    
    class NewReservationCallback extends AbstractCallback<Reservation> {
        public NewReservationCallback(Reservation r) {
            super(Transaction.NEW_RESERVATION, r);
        }
        @Override
        public void clientCallbackImpl(ClientResponse clientResponse) {
            VoltTable[] results = clientResponse.getResults();
            
            BitSet seats = getSeatsBitSet(element.flight_id);
            
            // Valid NewReservation
            if (clientResponse.getStatus() == Status.OK) {
                assert(results.length > 1);
                assert(results[0].getRowCount() == 1);
                assert(results[0].asScalarLong() == 1);

                // Mark this seat as successfully reserved
                seats.set(element.seatnum);

                // Set it up so we can play with it later
                SEATSClient.this.requeueReservation(element);
            }
            // Aborted - Figure out why!
            else if (clientResponse.getStatus() == Status.ABORT_USER) {
                String msg = clientResponse.getStatusString();
                ErrorType errorType = ErrorType.getErrorType(msg);
                
                if (debug.get())
                    LOG.debug(String.format("Client %02d :: NewReservation %s [ErrorType=%s] - %s",
                                       getClientId(), clientResponse.getStatus(), errorType, clientResponse.getStatusString()),
                                       clientResponse.getException());
                
                newReservationErrors.put(errorType.name());
                switch (errorType) {
                    case NO_MORE_SEATS: {
                        seats.set(0, SEATSConstants.NUM_SEATS_PER_FLIGHT);
                        if (debug.get())
                            LOG.debug(String.format("FULL FLIGHT: %s", element.flight_id));                        
                        break;
                    }
                    case CUSTOMER_ALREADY_HAS_SEAT: {
                        Set<FlightId> f_ids = getCustomerBookedFlights(element.customer_id);
                        f_ids.add(element.flight_id);
                        if (debug.get())
                            LOG.debug(String.format("ALREADY BOOKED: %s -> %s", element.customer_id, f_ids));
                        break;
                    }
                    case SEAT_ALREADY_RESERVED: {
                        seats.set(element.seatnum);
                        if (debug.get())
                            LOG.debug(String.format("ALREADY BOOKED SEAT: %s/%d -> %s",
                                                    element.customer_id, element.seatnum, seats));
                        break;
                    }
                    case INVALID_CUSTOMER_ID: {
                        LOG.warn("Unexpected invalid CustomerId: " + element.customer_id);
                        break;
                    }
                    case INVALID_FLIGHT_ID: {
                        LOG.warn("Unexpected invalid FlightId: " + element.flight_id);
                        break;
                    }
                    case UNKNOWN: {
//                        if (debug.get()) 
                            LOG.warn(msg);
                        break;
                    }
                    default: {
                        if (debug.get()) LOG.debug("BUSTED ID: " + element.flight_id + " / " + element.flight_id.encode());
                    }
                } // SWITCH
            }
        }
    }
    
    private boolean executeNewReservation(Transaction txn) throws IOException {
        this.startComputeTime(txn.displayName);
        Reservation reservation = null;
        BitSet seats = null;
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_INSERTS);
        assert(cache != null) : "Unexpected " + CacheType.PENDING_INSERTS;
        
        if (debug.get()) LOG.debug(String.format("Attempting to get a new pending insert Reservation [totalPendingInserts=%d]",
                                                 cache.size()));
        while (reservation == null) {
            Reservation r = null;
            CacheType.PENDING_INSERTS.lock.lock();
            try {
                r = cache.poll();
            } finally {
                CacheType.PENDING_INSERTS.lock.unlock();
            } // SYNCH
            if (r == null) break;
            
            seats = SEATSClient.getSeatsBitSet(r.flight_id);
            
            if (isFlightFull(seats)) {
                if (debug.get()) LOG.debug(String.format("%s is full", r.flight_id));
                continue;
            }
            else if (seats.get(r.seatnum)) {
                if (debug.get()) LOG.debug(String.format("Seat #%d on %s is already booked", r.seatnum, r.flight_id));
                continue;
            }
            else if (isCustomerBookedOnFlight(r.customer_id, r.flight_id)) {
                if (debug.get()) LOG.debug(String.format("%s is already booked on %s", r.customer_id, r.flight_id));
                continue;
            }
            reservation = r; 
        } // WHILE
        if (reservation == null) {
            if (debug.get()) LOG.debug("Failed to find a valid pending insert Reservation\n" + this.toString());
            this.stopComputeTime(txn.displayName);
            return (false);
        }
        
        // Generate a random price for now
        double price = 2.0 * rng.number(SEATSConstants.MIN_RESERVATION_PRICE,
                                        SEATSConstants.MAX_RESERVATION_PRICE);
        
        // Generate random attributes
        long attributes[] = new long[9];
        for (int i = 0; i < attributes.length; i++) {
            attributes[i] = rng.nextLong();
        } // FOR
        
        Object params[] = new Object[] {
                reservation.id,
                reservation.customer_id.encode(),
                reservation.flight_id.encode(),
                reservation.seatnum,
                price,
                attributes
        };
        if (trace.get()) LOG.trace("Calling " + txn.getExecName());
        this.stopComputeTime(txn.displayName);
        this.getClientHandle().callProcedure(new NewReservationCallback(reservation),
                                             txn.getExecName(),
                                             params);
        
        return (true);
    }

    // ----------------------------------------------------------------
    // UpdateCustomer
    // ----------------------------------------------------------------
    
    class UpdateCustomerCallback extends AbstractCallback<CustomerId> {
        public UpdateCustomerCallback(CustomerId c) {
            super(Transaction.UPDATE_CUSTOMER, c);
        }
        @Override
        public void clientCallbackImpl(ClientResponse clientResponse) {
            VoltTable[] results = clientResponse.getResults();
            if (clientResponse.getStatus() == Status.OK) {
                assert (results.length >= 1);
                assert (results[0].getRowCount() == 1);
//                assert (results[0].asScalarLong() == 1);
            } else if (debug.get()) {
                LOG.debug("UpdateCustomer " + ": " + clientResponse.getStatusString(), clientResponse.getException());
            }
        }
    }

    private boolean executeUpdateCustomer(Transaction txn) throws IOException {
        this.startComputeTime(txn.displayName);
        
        // Pick a random customer and then have at it!
        CustomerId customer_id = this.getRandomCustomerId();
        long attr0 = this.rng.nextLong();
        long attr1 = this.rng.nextLong();
        long update_ff = (rng.number(1, 100) <= SEATSConstants.PROB_UPDATE_FREQUENT_FLYER ? 1 : 0);
        
        Object params[] = new Object[]{
            VoltType.NULL_BIGINT,
            "",
            update_ff,
            attr0,
            attr1
        };
        
        // Update with the Customer's id as a string 
        if (rng.nextInt(100) < SEATSConstants.PROB_UPDATE_WITH_CUSTOMER_ID_STR) {
            params[1] = Long.toString(customer_id.encode());
        }
        // Update using their Customer id
        else {
            params[0] = customer_id.encode();
        }

        if (trace.get()) LOG.trace("Calling " + txn.getExecName());
        this.stopComputeTime(txn.displayName);
        this.getClientHandle().callProcedure(new UpdateCustomerCallback(customer_id),
                                             txn.getExecName(),
                                             params);
        return (true);
    }

    // ----------------------------------------------------------------
    // UpdateReservation
    // ----------------------------------------------------------------
    
    class UpdateReservationCallback extends AbstractCallback<Reservation> {
        public UpdateReservationCallback(Reservation r) {
            super(Transaction.UPDATE_RESERVATION, r);
        }
        @Override
        public void clientCallbackImpl(ClientResponse clientResponse) {
            if (clientResponse.getStatus() == Status.OK) {
                assert (clientResponse.getResults().length == 1);
                assert (clientResponse.getResults()[0].getRowCount() == 1);
                assert (clientResponse.getResults()[0].asScalarLong() == 1 ||
                        clientResponse.getResults()[0].asScalarLong() == 0);
                
                SEATSClient.this.requeueReservation(element);
            }
        }
    }

    private boolean executeUpdateReservation(Transaction txn) throws IOException {
        this.startComputeTime(txn.displayName);
        
        LinkedList<Reservation> cache = CACHE_RESERVATIONS.get(CacheType.PENDING_UPDATES);
        assert(cache != null) : "Unexpected " + CacheType.PENDING_UPDATES;
        
        // Pull off the first pending seat change and throw that ma at the server
        Reservation r = null;
        CacheType.PENDING_UPDATES.lock.lock();
        try {
            r = cache.poll();
        } finally {
            CacheType.PENDING_UPDATES.lock.unlock();
        } // SYNCH
        if (r == null) {
            this.stopComputeTime(txn.displayName);
            return (false);
        }
        
        // Pick a random reservation id
        long value = rng.number(1, 1 << 20);
        long attribute_idx = rng.nextInt(UpdateReservation.NUM_UPDATES);

        Object params[] = new Object[] {
                r.id,
                r.flight_id.encode(),
                r.customer_id.encode(),
                r.seatnum,
                attribute_idx,
                value
        };
        
        if (trace.get()) LOG.trace("Calling " + txn.getExecName());
        this.stopComputeTime(txn.displayName);
        this.getClientHandle().callProcedure(new UpdateReservationCallback(r),
                                             txn.getExecName(), 
                                             params);
        return (true);
    }
}