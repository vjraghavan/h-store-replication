/***************************************************************************
 *  Copyright (C) 2010 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Visawee Angkanawaraphan (visawee@cs.brown.edu)                         *
 *  http://www.cs.brown.edu/~visawee/                                      *
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
package edu.brown.benchmark.auctionmark;

import java.io.File;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.benchmark.auctionmark.util.AuctionMarkCategoryParser;
import edu.brown.benchmark.auctionmark.util.Category;
import edu.brown.benchmark.auctionmark.util.CompositeId;
import edu.brown.benchmark.auctionmark.util.ItemId;
import edu.brown.benchmark.auctionmark.util.ItemInfo;
import edu.brown.benchmark.auctionmark.util.UserId;
import edu.brown.benchmark.auctionmark.util.UserIdGenerator;
import edu.brown.benchmark.auctionmark.util.ItemInfo.Bid;
import edu.brown.catalog.CatalogUtil;
import edu.brown.rand.RandomDistribution.Flat;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservableExceptionHandler;
import edu.brown.utils.EventObserver;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

/**
 * 
 * @author pavlo
 * @author visawee
 */
public class AuctionMarkLoader extends AuctionMarkBaseClient {
    private static final Logger LOG = Logger.getLogger(AuctionMarkLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * Data Generator Classes
     * TableName -> AbstactTableGenerator
     */
    private final Map<String, AbstractTableGenerator> generators = new ListOrderedMap<String, AbstractTableGenerator>();
    
    private final Collection<String> sub_generators = new HashSet<String>();

    /** The set of tables that we have finished loading **/
    private final transient Collection<String> finished = new HashSet<String>();
    
    /**
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        edu.brown.benchmark.BenchmarkComponent.main(AuctionMarkLoader.class, args, true);
    }

    /**
     * Constructor
     * 
     * @param args
     */
    public AuctionMarkLoader(String[] args) {
        super(AuctionMarkLoader.class, args);

        if (debug.get()) LOG.debug("AuctionMarkLoader::: numClients = " + this.getNumClients());
        
        // ---------------------------
        // Fixed-Size Table Generators
        // ---------------------------
        
        this.registerGenerator(new RegionGenerator());
        this.registerGenerator(new CategoryGenerator(new File(this.data_directory, "categories.txt")));
        this.registerGenerator(new GlobalAttributeGroupGenerator());
        this.registerGenerator(new GlobalAttributeValueGenerator());

        // ---------------------------
        // Scaling-Size Table Generators
        // ---------------------------
        
        // USER TABLES
        this.registerGenerator(new UserGenerator());
        this.registerGenerator(new UserAttributesGenerator());
        this.registerGenerator(new UserItemGenerator());
        this.registerGenerator(new UserWatchGenerator());
        this.registerGenerator(new UserFeedbackGenerator());
        
        // ITEM TABLES
        this.registerGenerator(new ItemGenerator());
        this.registerGenerator(new ItemAttributeGenerator());
        this.registerGenerator(new ItemBidGenerator());
        this.registerGenerator(new ItemMaxBidGenerator());
        this.registerGenerator(new ItemCommentGenerator());
        this.registerGenerator(new ItemImageGenerator());
        this.registerGenerator(new ItemPurchaseGenerator());
    }
    
    private void registerGenerator(AbstractTableGenerator generator) {
        // Register this one as well as any sub-generators
        this.generators.put(generator.getTableName(), generator);
        for (AbstractTableGenerator sub_generator : generator.getSubTableGenerators()) {
            this.registerGenerator(sub_generator);
            this.sub_generators.add(sub_generator.getTableName());
        } // FOR
    }

    @Override
    public String[] getTransactionDisplayNames() {
        return new String[] {};
    }

    /**
     * Call by the benchmark framework to load the table data
     */
    @Override
    public void runLoop() {
        final EventObservableExceptionHandler handler = new EventObservableExceptionHandler();
        final List<Thread> threads = new ArrayList<Thread>();
        for (AbstractTableGenerator generator : this.generators.values()) {
            // if (isSubGenerator(generator)) continue;
            Thread t = new Thread(generator);
            t.setUncaughtExceptionHandler(handler);
            threads.add(t);
            
            // Make sure we call init first before starting any thread
            generator.init();
        }
        assert(threads.size() > 0);
        handler.addObserver(new EventObserver() {
            @Override
            public void update(Observable o, Object obj) {
                for (Thread t : threads)
                    t.interrupt();
            }
        });
        
        // Construct a new thread to load each table
        // Fire off the threads and wait for them to complete
        // If debug is set to true, then we'll execute them serially
        try {
            for (Thread t : threads) {
                t.start();
            } // FOR
            for (Thread t : threads) {
                t.join();
            } // FOR
        } catch (InterruptedException e) {
            LOG.fatal("Unexpected error", e);
        } finally {
            if (handler.hasError()) {
                throw new RuntimeException("Error while generating table data.", handler.getError());
            }
        }
        
        this.saveProfile();
        LOG.info("Finished generating data for all tables");
        if (debug.get()) LOG.debug("Table Sizes:\n" + profile.table_sizes);
    }

    /**
     * Load the tuples for the given table name
     * @param tableName
     */
    protected void generateTableData(String tableName) {
        LOG.info("*** START " + tableName);
        final AbstractTableGenerator generator = this.generators.get(tableName);
        assert (generator != null);

        // Generate Data
        final VoltTable volt_table = generator.getVoltTable();
        while (generator.hasMore()) {
            generator.generateBatch();
            this.loadTable(generator.getTableName(), volt_table, generator.getTableSize());
            volt_table.clearRowData();
        } // WHILE
        
        // Mark as finished
        generator.markAsFinished();
        this.finished.add(tableName);
        LOG.info(String.format("*** FINISH %s - %d tuples - [%d / %d]", tableName, this.profile.getTableSize(tableName), this.finished.size(), this.generators.size()));
        if (debug.get()) {
            LOG.debug("Remaining Tables: " + CollectionUtils.subtract(this.generators.keySet(), this.finished));
        }
        
    }
    
    /**
     * The method that actually loads the VoltTable into the database Can be overridden for testing purposes.
     * 
     * @param tableName
     * @param table
     */
    protected void loadTable(String tableName, VoltTable table, Long expected) {
        long count = table.getRowCount();
        long current = this.profile.getTableSize(tableName);
        
        if (debug.get()) 
            LOG.debug(String.format("%s: Loading %d rows - TOTAL %d%s [bytes=%d]",
                                    tableName, count, current,
                                    (expected != null && expected > 0 ? " / " + expected : ""),
                                    table.getUnderlyingBufferSize()));

        // Load up this dirty mess...
        try {
            this.getClientHandle().callProcedure("@LoadMultipartitionTable", tableName, table);
        } catch (Exception e) {
            throw new RuntimeException("Error when trying load data for '" + tableName + "'", e);
        }
        this.profile.addToTableSize(tableName, count);
    }


    /**********************************************************************************************
     * AbstractTableGenerator
     **********************************************************************************************/
    protected abstract class AbstractTableGenerator implements Runnable {
        private final String tableName;
        private final Table catalog_tbl;
        protected final VoltTable table;
        protected Long tableSize;
        protected Long batchSize;
        protected final CountDownLatch latch = new CountDownLatch(1);
        protected final List<String> dependencyTables = new ArrayList<String>();

        /**
         * Some generators have children tables that we want to load tuples for each batch of this generator. 
         * The queues we need to update every time we generate a new ItemInfo
         */
        protected final Set<SubTableGenerator<?>> sub_generators = new HashSet<SubTableGenerator<?>>();  

        protected final Object[] row;
        protected long count = 0;
        
        /** Any column with the name XX_SATTR## will automatically be filled with a random string */
        protected final List<Column> random_str_cols = new ArrayList<Column>();
        protected final Pattern random_str_regex = Pattern.compile("[\\w]+\\_SATTR[\\d]+", Pattern.CASE_INSENSITIVE);
        
        /** Any column with the name XX_IATTR## will automatically be filled with a random integer */
        protected List<Column> random_int_cols = new ArrayList<Column>();
        protected final Pattern random_int_regex = Pattern.compile("[\\w]+\\_IATTR[\\d]+", Pattern.CASE_INSENSITIVE);

        /**
         * Constructor
         * 
         * @param catalog_tbl
         */
        public AbstractTableGenerator(String tableName, String...dependencies) {
            this.tableName = tableName;
            this.catalog_tbl = AuctionMarkLoader.this.getTableCatalog(tableName);
            assert(catalog_tbl != null) : "Invalid table name '" + tableName + "'";
            
            boolean fixed_size = AuctionMarkConstants.FIXED_TABLES.contains(catalog_tbl.getName());
            boolean dynamic_size = AuctionMarkConstants.DYNAMIC_TABLES.contains(catalog_tbl.getName());
            boolean data_file = AuctionMarkConstants.DATAFILE_TABLES.contains(catalog_tbl.getName());

            // Generate a VoltTable instance we can use
            this.table = CatalogUtil.getVoltTable(catalog_tbl);
            this.row = new Object[this.table.getColumnCount()];

            // Add the dependencies so that we know what we need to block on
            CollectionUtil.addAll(this.dependencyTables, dependencies);
            
            try {
                String field_name = "BATCHSIZE_" + catalog_tbl.getName();
                Field field_handle = AuctionMarkConstants.class.getField(field_name);
                assert (field_handle != null);
                this.batchSize = (Long) field_handle.get(null);
            } catch (Exception ex) {
                throw new RuntimeException("Missing field needed for '" + tableName + "'", ex);
            } 

            // Initialize dynamic parameters for tables that are not loaded from data files
            if (!data_file && !dynamic_size && tableName.equalsIgnoreCase(AuctionMarkConstants.TABLENAME_ITEM) == false) {
                try {
                    String field_name = "TABLESIZE_" + catalog_tbl.getName();
                    Field field_handle = AuctionMarkConstants.class.getField(field_name);
                    assert (field_handle != null);
                    this.tableSize = (Long) field_handle.get(null);
                    if (!fixed_size) {
                        this.tableSize = Math.round(this.tableSize / AuctionMarkLoader.this.profile.getScaleFactor());
                    }
                } catch (NoSuchFieldException ex) {
                    if (debug.get()) LOG.warn("No table size field for '" + tableName + "'", ex);
                } catch (Exception ex) {
                    throw new RuntimeException("Missing field needed for '" + tableName + "'", ex);
                } 
            } 
            
            for (Column catalog_col : this.catalog_tbl.getColumns()) {
                if (random_str_regex.matcher(catalog_col.getName()).matches()) {
                    assert(catalog_col.getType() == VoltType.STRING.getValue()) : catalog_col.fullName();
                    this.random_str_cols.add(catalog_col);
                    if (trace.get()) LOG.trace("Random String Column: " + catalog_col.fullName());
                }
                else if (random_int_regex.matcher(catalog_col.getName()).matches()) {
                    assert(catalog_col.getType() != VoltType.STRING.getValue()) : catalog_col.fullName();
                    this.random_int_cols.add(catalog_col);
                    if (trace.get()) LOG.trace("Random Integer Column: " + catalog_col.fullName());
                }
            } // FOR
            if (debug.get()) {
                if (this.random_str_cols.size() > 0) LOG.debug(String.format("%s Random String Columns: %s", tableName, CatalogUtil.debug(this.random_str_cols)));
                if (this.random_int_cols.size() > 0) LOG.debug(String.format("%s Random Integer Columns: %s", tableName, CatalogUtil.debug(this.random_int_cols)));
            }
        }

        /**
         * Initiate data that need dependencies
         */
        public abstract void init();
        
        /**
         * All sub-classes must implement this. This will enter new tuple data into the row
         */
        protected abstract int populateRow();
        
        public void run() {
            // First block on the CountDownLatches of all the tables that we depend on
            if (this.dependencyTables.size() > 0 && debug.get())
                LOG.debug(String.format("%s: Table generator is blocked waiting for %d other tables: %s",
                                        this.tableName, this.dependencyTables.size(), this.dependencyTables));
            for (String dependency : this.dependencyTables) {
                AbstractTableGenerator gen = AuctionMarkLoader.this.generators.get(dependency);
                assert(gen != null) : "Missing table generator for '" + dependency + "'";
                try {
                    gen.latch.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException("Unexpected interruption for '" + this.tableName + "' waiting for '" + dependency + "'", ex);
                }
            } // FOR
            
            // Then invoke the loader generation method
            try {
                AuctionMarkLoader.this.generateTableData(this.tableName);
            } catch (Throwable ex) {
                throw new RuntimeException("Unexpected error while generating table data for '" + this.tableName + "'", ex);
            }
        }
        
        @SuppressWarnings("unchecked")
        public <T extends AbstractTableGenerator> T addSubTableGenerator(SubTableGenerator<?> sub_item) {
            this.sub_generators.add(sub_item);
            return ((T)this);
        }
        @SuppressWarnings("unchecked")
        public void updateSubTableGenerators(Object obj) {
            // Queue up this item for our multi-threaded sub-generators
            if (trace.get())
                LOG.trace(String.format("%s: Updating %d sub-generators with %s: %s",
                                        this.tableName, this.sub_generators.size(), obj, this.sub_generators));
            for (SubTableGenerator sub_generator : this.sub_generators) {
                sub_generator.queue(obj);
            } // FOR
        }
        public boolean hasSubTableGenerators() {
            return (!this.sub_generators.isEmpty());
        }
        public Collection<SubTableGenerator<?>> getSubTableGenerators() {
            return (this.sub_generators);
        }
        public Collection<String> getSubGeneratorTableNames() {
            List<String> names = new ArrayList<String>();
            for (AbstractTableGenerator gen : this.sub_generators) {
                names.add(gen.catalog_tbl.getName());
            }
            return (names);
        }
        
        protected int populateRandomColumns(Object row[]) {
            int cols = 0;
            
            // STRINGS
            for (Column catalog_col : this.random_str_cols) {
                int size = catalog_col.getSize();
                row[catalog_col.getIndex()] = rng.astring(rng.nextInt(size - 1), size);
                cols++;
            } // FOR
            
            // INTEGER
            for (Column catalog_col : this.random_int_cols) {
                row[catalog_col.getIndex()] = rng.number(0, 1<<30);
                cols++;
            } // FOR
            
            return (cols);
        }

        /**
         * Returns true if this generator has more tuples that it wants to add
         * @return
         */
        public synchronized boolean hasMore() {
            return (this.count < this.tableSize);
        }
        /**
         * Return the table's catalog object for this generator
         * @return
         */
        public Table getTableCatalog() {
            return (this.catalog_tbl);
        }
        /**
         * Return the VoltTable handle
         * @return
         */
        public VoltTable getVoltTable() {
            return this.table;
        }
        /**
         * Returns the number of tuples that will be loaded into this table
         * @return
         */
        public Long getTableSize() {
            return this.tableSize;
        }
        /**
         * Returns the number of tuples per batch that this generator will want loaded
         * @return
         */
        public Long getBatchSize() {
            return this.batchSize;
        }
        /**
         * Returns the name of the table this this generates
         * @return
         */
        public String getTableName() {
            return this.tableName;
        }
        /**
         * Returns the total number of tuples generated thusfar
         * @return
         */
        public synchronized long getCount() {
            return this.count;
        }

        /**
         * When called, the generator will populate a new row record and append it to the underlying VoltTable
         */
        public synchronized void addRow() {
            int cols = this.populateRow();
            // RANDOM COLS
            cols += populateRandomColumns(this.row);
            
            assert(cols == this.table.getColumnCount()) : String.format("Invalid number of columns for %s [expected=%d, actual=%d]",
                                                                        this.tableName, this.table.getColumnCount(), cols);
            
            // Convert all CompositeIds into their long encodings
            for (int i = 0; i < cols; i++) {
                if (this.row[i] != null && this.row[i] instanceof CompositeId) {
                    this.row[i] = ((CompositeId)this.row[i]).encode();
                }
            } // FOR
            
            this.count++;
            this.table.addRow(this.row);
        }
        /**
         * 
         */
        public void generateBatch() {
            if (trace.get()) LOG.trace(String.format("%s: Generating new batch", this.getTableName()));
            long batch_count = 0;
            while (this.hasMore() && this.table.getRowCount() < this.batchSize) {
                this.addRow();
                batch_count++;
            } // WHILE
            if (debug.get()) LOG.debug(String.format("%s: Finished generating new batch of %d tuples", this.getTableName(), batch_count));
        }

        public void markAsFinished(){
        	this.latch.countDown();
            for (SubTableGenerator<?> sub_generator : this.sub_generators) {
                sub_generator.stopWhenEmpty();
            } // FOR
        }
        
        public boolean isFinish(){
        	return (this.latch.getCount() == 0);
        }
        
        public List<String> getDependencies() {
            return this.dependencyTables;
        }
        
        @Override
        public String toString() {
            return String.format("Generator[%s]", this.tableName);
        }
    } // END CLASS

    /**********************************************************************************************
     * SubUserTableGenerator
     * This is for tables that are based off of the USER table
     **********************************************************************************************/
    protected abstract class SubTableGenerator<T> extends AbstractTableGenerator {
        
        private final LinkedBlockingDeque<T> queue = new LinkedBlockingDeque<T>();
        private T current;
        private short currentCounter;
        private boolean stop = false;
        private final String sourceTableName;

        public SubTableGenerator(String tableName, String sourceTableName, String...dependencies) {
            super(tableName, dependencies);
            this.sourceTableName = sourceTableName;
        }
        
        protected abstract short getElementCounter(T t);
        protected abstract int populateRow(T t, short remaining);
        
        public void queue(T t) {
            assert(this.queue.contains(t) == false) : "Trying to queue duplicate element for '" + this.getTableName() + "'";
            this.queue.add(t);
        }
        public void stopWhenEmpty() {
            this.stop = true;
        }
        
        @Override
        public void init() {
            // Get the AbstractTableGenerator that will feed into this generator
            AbstractTableGenerator parent_gen = AuctionMarkLoader.this.generators.get(this.sourceTableName);
            assert(parent_gen != null) : "Unexpected source TableGenerator '" + this.sourceTableName + "'";
            parent_gen.addSubTableGenerator(this);
            
            this.current = null;
            this.currentCounter = 0;
        }
        @Override
        public final boolean hasMore() {
            return (this.getNext() != null);
        }
        @Override
        protected final int populateRow() {
            T t = this.getNext();
            assert(t != null);
            this.currentCounter--;
            return (this.populateRow(t, this.currentCounter));
        }
        private final T getNext() {
            T last = this.current;
            if (this.current == null || this.currentCounter == 0) {
                while (this.currentCounter == 0) {
                    try {
                        this.current = this.queue.poll(1000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ex) {
                        return (null);
                    }
                    // Check whether we should stop
                    if (this.current == null) {
                        if (this.stop) break;
                        continue;
                    }
                    this.currentCounter = this.getElementCounter(this.current);
                } // WHILE
            }
            if (last != this.current) {
                if (last != null) this.finishElementCallback(last);
                if (this.current != null) this.newElementCallback(this.current);
            }
            return this.current;
        }
        protected void finishElementCallback(T t) {
            // Nothing...
        }
        protected void newElementCallback(T t) {
            // Nothing... 
        }
    } // END CLASS
    
    /**********************************************************************************************
     * REGION Generator
     **********************************************************************************************/
    protected class RegionGenerator extends AbstractTableGenerator {

        public RegionGenerator() {
            super(AuctionMarkConstants.TABLENAME_REGION);
        }

        @Override
        public void init() {
            // Nothing to do
        }
        @Override
        protected int populateRow() {
            int col = 0;

            // R_ID
            this.row[col++] = new Integer((int) this.count);
            // R_NAME
            this.row[col++] = AuctionMarkLoader.this.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * CATEGORY Generator
     **********************************************************************************************/
    protected class CategoryGenerator extends AbstractTableGenerator {
        private final File data_file;
        private final Map<String, Category> categoryMap;
        private Iterator<String> categoryKeyItr;

        public CategoryGenerator(File data_file) {
            super(AuctionMarkConstants.TABLENAME_CATEGORY);
            this.data_file = data_file;

            assert (this.data_file.exists()) : "The data file for the category generator does not exist: " + this.data_file;

            this.categoryMap = (new AuctionMarkCategoryParser(data_file)).getCategoryMap();
            this.categoryKeyItr = this.categoryMap.keySet().iterator();
            this.tableSize = new Long(this.categoryMap.size());
        }

        @Override
        public void init() {
            // Nothing to do
        }
        @Override
        protected int populateRow() {
            int col = 0;

            String category_key = this.categoryKeyItr.next();
            Category category = this.categoryMap.get(category_key);

            Long category_id = new Long(category.getCategoryID());
            String category_name = category.getName();
            Long parent_id = new Long(category.getParentCategoryID());
            Long item_count = new Long(category.getItemCount());
            // this.name_id_xref.put(category_name, this.count);

            boolean leaf_node = category.isLeaf();
            if (leaf_node) {
                AuctionMarkLoader.this.profile.item_category_histogram.put(category_id, item_count.intValue());
            }

            // C_ID
            this.row[col++] = category_id;
            // C_NAME
            this.row[col++] = category_name;
            // C_PARENT_ID
            this.row[col++] = parent_id;
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * GLOBAL_ATTRIBUTE_GROUP Generator
     **********************************************************************************************/
    protected class GlobalAttributeGroupGenerator extends AbstractTableGenerator {
        private long num_categories = 0l;

        public GlobalAttributeGroupGenerator() {
            super(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP,
                  AuctionMarkConstants.TABLENAME_CATEGORY);
        }

        @Override
        public void init() {
            // Grab the number of CATEGORY items that we have inserted
            this.num_categories = AuctionMarkLoader.this.profile.getTableSize(AuctionMarkConstants.TABLENAME_CATEGORY);
        }
        @Override
        protected int populateRow() {
            int col = 0;

            // GAG_ID
            this.row[col++] = new Integer((int) this.count);
            // GAG_C_ID
            this.row[col++] = AuctionMarkLoader.this.rng.number(0, (int)this.num_categories);
            // GAG_NAME
            this.row[col++] = AuctionMarkLoader.this.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * GLOBAL_ATTRIBUTE_VALUE Generator
     **********************************************************************************************/
    protected class GlobalAttributeValueGenerator extends AbstractTableGenerator {

        private Zipf zipf;

        public GlobalAttributeValueGenerator() {
            super(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE,
                  AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP);
            this.zipf = new Zipf(AuctionMarkLoader.this.rng, 0, (int) AuctionMarkConstants.TABLESIZE_GLOBAL_ATTRIBUTE_GROUP, 1.001);
        }

        @Override
        public void init() {
            // Nothing to do
        }
        @Override
        protected int populateRow() {
            int col = 0;

            long GAV_ID = new Integer((int) this.count);
            long GAV_GAG_ID = this.zipf.nextInt(); 
            
            profile.addGAGIdGAVIdPair(GAV_GAG_ID, GAV_ID);
            
            // GAV_ID
            this.row[col++] = GAV_ID;
            // GAV_GAG_ID
            this.row[col++] = GAV_GAG_ID;
            // GAV_NAME
            this.row[col++] = AuctionMarkLoader.this.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * USER Generator
     **********************************************************************************************/
    protected class UserGenerator extends AbstractTableGenerator {
        private final Zipf randomBalance;
        private final Flat randomRegion;
        private final Zipf randomRating;
        private UserIdGenerator idGenerator;
        
        public UserGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER,
                  AuctionMarkConstants.TABLENAME_REGION);
            this.randomRegion = new Flat(AuctionMarkLoader.this.rng, 0, (int) AuctionMarkConstants.TABLESIZE_REGION);
            this.randomRating = new Zipf(AuctionMarkLoader.this.rng, AuctionMarkConstants.USER_MIN_RATING,
                                                                     AuctionMarkConstants.USER_MAX_RATING, 1.0001);
            this.randomBalance = new Zipf(AuctionMarkLoader.this.rng, AuctionMarkConstants.USER_MIN_BALANCE,
                                                                      AuctionMarkConstants.USER_MAX_BALANCE, 1.001);
        }

        @Override
        public void init() {
            // Populate the profile's users per item count histogram so that we know how many
            // items that each user should have. This will then be used to calculate the
            // the user ids by placing them into numeric ranges
            Zipf randomNumItems = new Zipf(rng,
                                           AuctionMarkConstants.ITEM_MIN_ITEMS_PER_SELLER,
                                           Math.round(AuctionMarkConstants.ITEM_MAX_ITEMS_PER_SELLER / profile.getScaleFactor()),
                                           1.001);
            for (long i = 0; i < this.tableSize; i++) {
                long num_items = randomNumItems.nextInt();
                profile.users_per_item_count.put(num_items);
            } // FOR
            if (trace.get())
                LOG.trace("Users Per Item Count:\n" + profile.users_per_item_count);
            this.idGenerator = new UserIdGenerator(profile.users_per_item_count, getNumClients());
            assert(this.idGenerator.hasNext());
        }
        @Override
        public synchronized boolean hasMore() {
            return this.idGenerator.hasNext();
        }
        @Override
        protected int populateRow() {
            int col = 0;

            UserId u_id = this.idGenerator.next();
            
            // U_ID
            this.row[col++] = u_id;
            // U_RATING
            this.row[col++] = this.randomRating.nextInt();
            // U_BALANCE
            this.row[col++] = (this.randomBalance.nextInt()) / 10.0;
            // U_COMMENTS
            this.row[col++] = 0;
            // U_R_ID
            this.row[col++] = this.randomRegion.nextInt();
            // U_CREATED
            this.row[col++] = VoltTypeUtil.getRandomValue(VoltType.TIMESTAMP);
            // U_UPDATED
            this.row[col++] = VoltTypeUtil.getRandomValue(VoltType.TIMESTAMP);
            
            this.updateSubTableGenerators(u_id);
            return (col);
        }
    }

    /**********************************************************************************************
     * USER_ATTRIBUTES Generator
     **********************************************************************************************/
    protected class UserAttributesGenerator extends SubTableGenerator<UserId> {
        private final Zipf randomNumUserAttributes;
        
        public UserAttributesGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_ATTRIBUTES,
                  AuctionMarkConstants.TABLENAME_USER);
            
            this.randomNumUserAttributes = new Zipf(rng, AuctionMarkConstants.USER_MIN_ATTRIBUTES,
                                                         AuctionMarkConstants.USER_MAX_ATTRIBUTES, 1.001);
        }
        @Override
        protected short getElementCounter(UserId user_id) {
            return (short)(randomNumUserAttributes.nextInt());
        }
        @Override
        protected int populateRow(UserId user_id, short remaining) {
            int col = 0;
            
            // UA_ID
            this.row[col++] = this.count;
            // UA_U_ID
            this.row[col++] = user_id;
            // UA_NAME
            this.row[col++] = AuctionMarkLoader.this.rng.astring(5, 32);
            // UA_VALUE
            this.row[col++] = AuctionMarkLoader.this.rng.astring(5, 32);
            // U_CREATED
            this.row[col++] = VoltTypeUtil.getRandomValue(VoltType.TIMESTAMP);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * ITEM Generator
     **********************************************************************************************/
    protected class ItemGenerator extends SubTableGenerator<UserId> {

        /** Current time in milliseconds */
        private final long currentTimestamp;
        
        /**
         * BidDurationDay -> Pair<NumberOfBids, NumberOfWatches>
         */
        private final Map<Long, Pair<Zipf, Zipf>> item_bid_watch_zipfs = new HashMap<Long, Pair<Zipf,Zipf>>();
        
        public ItemGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM,
                  AuctionMarkConstants.TABLENAME_USER,
                  AuctionMarkConstants.TABLENAME_CATEGORY);
            
            long ct1 = Calendar.getInstance().getTimeInMillis();
            long ct2 = Math.round((double)ct1 / 1000);
            long ct3 = ct2 * 1000;
            this.currentTimestamp = ct3;
        }
        
        @Override
        protected short getElementCounter(UserId user_id) {
            return (short)(user_id.getItemCount());
        }

        @Override
        public void init() {
            super.init();
            this.tableSize = 0l;
            for (Long size : profile.users_per_item_count.values()) {
                this.tableSize += size.intValue() * profile.users_per_item_count.get(size);
            } // FOR
        }
        @Override
        protected int populateRow(UserId seller_id, short remaining) {
            int col = 0;
            
            ItemId itemId = new ItemId(seller_id, remaining);
            ItemInfo itemInfo = new ItemInfo(itemId);
            itemInfo.sellerId = seller_id;
            itemInfo.endDate = this.getRandomEndTimestamp();
            itemInfo.startDate = this.getRandomStartTimestamp(itemInfo.endDate);
            itemInfo.initialPrice = profile.randomInitialPrice.nextInt();
            assert(itemInfo.initialPrice > 0) : "Invalid initial price for " + itemId;
            itemInfo.numImages = (short) profile.randomNumImages.nextInt();
            itemInfo.numAttributes = (short) profile.randomNumAttributes.nextInt();
            
            //LOG.info("endDate = " + endDate + " : startDate = " + startDate);
            long bidDurationDay = ((itemInfo.endDate.getTime() - itemInfo.startDate.getTime()) / AuctionMarkConstants.MICROSECONDS_IN_A_DAY);
            if (this.item_bid_watch_zipfs.containsKey(bidDurationDay) == false) {
                Zipf randomNumBids = new Zipf(AuctionMarkLoader.this.rng,
                        AuctionMarkConstants.ITEM_MIN_BIDS_PER_DAY * (int)bidDurationDay,
                        AuctionMarkConstants.ITEM_MAX_BIDS_PER_DAY * (int)bidDurationDay,
                        1.001);
                Zipf randomNumWatches = new Zipf(AuctionMarkLoader.this.rng,
                        AuctionMarkConstants.ITEM_MIN_WATCHES_PER_DAY * (int)bidDurationDay,
                        (int)Math.ceil(AuctionMarkConstants.ITEM_MAX_WATCHES_PER_DAY * (int)bidDurationDay / profile.getScaleFactor()), 1.001);
                this.item_bid_watch_zipfs.put(bidDurationDay, Pair.of(randomNumBids, randomNumWatches));
            }
            Pair<Zipf, Zipf> p = this.item_bid_watch_zipfs.get(bidDurationDay);
            assert(p != null);
            
            // Calculate the number of bids and watches for this item
            itemInfo.numBids = (short)p.getFirst().nextInt();
            itemInfo.numWatches = (short)p.getSecond().nextInt();
            
            // The auction for this item has already closed
            if (itemInfo.endDate.getTime() <= this.currentTimestamp * 1000l) {
                itemInfo.stillAvailable = false;
                if (itemInfo.numBids > 0) {
                	// Somebody won a bid and bought the item
                    itemInfo.lastBidderId = profile.getRandomBuyerId(itemInfo.sellerId);
                    //System.out.println("@@@ z last_bidder_id = " + itemInfo.last_bidder_id);
                    itemInfo.purchaseDate = this.getRandomPurchaseTimestamp(itemInfo.endDate);
                    itemInfo.numComments = (short) profile.randomNumComments.nextInt();
                    profile.addCompleteItemId(itemInfo.id);
                }
            }
            // Item is still available
            else {
            	itemInfo.stillAvailable = true;
            	profile.addAvailableItemId(itemInfo.id);
            	if (itemInfo.numBids > 0) {
            		itemInfo.lastBidderId = profile.getRandomBuyerId(itemInfo.sellerId);
            	}
            }

            // I_ID
            this.row[col++] = itemInfo.id;
            // I_U_ID
            this.row[col++] = itemInfo.sellerId;
            // I_C_ID
            this.row[col++] = profile.getRandomCategoryId();
            // I_NAME
            this.row[col++] = AuctionMarkLoader.this.rng.astring(6, 32);
            // I_DESCRIPTION
            this.row[col++] = AuctionMarkLoader.this.rng.astring(50, 255);
            // I_USER_ATTRIBUTES
            this.row[col++] = AuctionMarkLoader.this.rng.astring(20, 255);
            // I_INITIAL_PRICE
            this.row[col++] = itemInfo.initialPrice;

            // I_CURRENT_PRICE
            if (itemInfo.numBids > 0) {
                itemInfo.currentPrice = itemInfo.initialPrice + (itemInfo.numBids * itemInfo.initialPrice * AuctionMarkConstants.ITEM_BID_PERCENT_STEP);
                this.row[col++] = itemInfo.currentPrice;
                itemId.setCurrentPrice(itemInfo.currentPrice); // HACK
            } else {
                this.row[col++] = itemInfo.initialPrice;
                itemId.setCurrentPrice(itemInfo.initialPrice); // HACK
            }

            // I_NUM_BIDS
            this.row[col++] = itemInfo.numBids;
            // I_NUM_IMAGES
            this.row[col++] = itemInfo.numImages;
            // I_NUM_GLOBAL_ATTRS
            this.row[col++] = itemInfo.numAttributes;
            // I_START_DATE
            this.row[col++] = itemInfo.startDate;
            // I_END_DATE
            this.row[col++] = itemInfo.endDate;
            // I_STATUS
            this.row[col++] = (itemInfo.stillAvailable ? AuctionMarkConstants.ITEM_STATUS_OPEN : AuctionMarkConstants.ITEM_STATUS_CLOSED);
            // I_UPDATED
            this.row[col++] = itemInfo.startDate;

            this.updateSubTableGenerators(itemInfo);
            return (col);
        }

        private TimestampType getRandomStartTimestamp(TimestampType endDate) {
            long duration = ((long)profile.randomDuration.nextInt()) * AuctionMarkConstants.MICROSECONDS_IN_A_DAY;
            long lStartTimestamp = endDate.getTime() - duration;
            TimestampType startTimestamp = new TimestampType(lStartTimestamp);
            return startTimestamp;
        }
        private TimestampType getRandomEndTimestamp() {
            int timeDiff = profile.randomTimeDiff.nextInt();
            return new TimestampType(this.currentTimestamp * 1000 + timeDiff * 1000 * 1000);
        }
        private TimestampType getRandomPurchaseTimestamp(TimestampType endDate) {
            long duration = profile.randomPurchaseDuration.nextInt();
            return new TimestampType(endDate.getTime() + duration * AuctionMarkConstants.MICROSECONDS_IN_A_DAY);
        }
    }
    
    /**********************************************************************************************
     * ITEM_IMAGE Generator
     **********************************************************************************************/
    protected class ItemImageGenerator extends SubTableGenerator<ItemInfo> {

        public ItemImageGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_IMAGE,
                  AuctionMarkConstants.TABLENAME_ITEM);
        }
        @Override
        public short getElementCounter(ItemInfo itemInfo) {
            return itemInfo.numImages;
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;

            // II_ID
            this.row[col++] = this.count;
            // II_I_ID
            this.row[col++] = itemInfo.id;
            // II_U_ID
            this.row[col++] = itemInfo.sellerId;

            return (col);
        }
    } // END CLASS
    
    /**********************************************************************************************
     * ITEM_ATTRIBUTE Generator
     **********************************************************************************************/
    protected class ItemAttributeGenerator extends SubTableGenerator<ItemInfo> {

        public ItemAttributeGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE,
                  AuctionMarkConstants.TABLENAME_ITEM,
                  AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP, AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE);
        }
        @Override
        public short getElementCounter(ItemInfo itemInfo) {
            return itemInfo.numAttributes;
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;
            Pair<Long, Long> gag_gav = profile.getRandomGAGIdGAVIdPair();
            assert(gag_gav != null);
            
            // IA_ID
            this.row[col++] = this.count;
            // IA_I_ID
            this.row[col++] = itemInfo.id;
            // IA_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IA_GAV_ID
            this.row[col++] = gag_gav.getSecond();
            // IA_GAG_ID
            this.row[col++] = gag_gav.getFirst();

            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * ITEM_COMMENT Generator
     **********************************************************************************************/
    protected class ItemCommentGenerator extends SubTableGenerator<ItemInfo> {

        public ItemCommentGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_COMMENT,
                  AuctionMarkConstants.TABLENAME_ITEM);
        }
        @Override
        public short getElementCounter(ItemInfo itemInfo) {
            return (itemInfo.purchaseDate != null ? itemInfo.numComments : 0);
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;

            // IC_ID
            this.row[col++] = new Integer((int) this.count);
            // IC_I_ID
            this.row[col++] = itemInfo.id;
            // IC_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IC_BUYER_ID
            this.row[col++] = itemInfo.lastBidderId;
            // IC_QUESTION
            this.row[col++] = AuctionMarkLoader.this.rng.astring(10, 128);
            // IC_RESPONSE
            this.row[col++] = AuctionMarkLoader.this.rng.astring(10, 128);
            // IC_CREATED
            this.row[col++] = this.getRandomCommentDate(itemInfo.startDate, itemInfo.endDate);
            // IC_UPDATED
            this.row[col++] = this.getRandomCommentDate(itemInfo.startDate, itemInfo.endDate);

            return (col);
        }
        private TimestampType getRandomCommentDate(TimestampType startDate, TimestampType endDate) {
            int start = Math.round(startDate.getTime() / 1000000);
            int end = Math.round(endDate.getTime() / 1000000);
            return new TimestampType((rng.number(start, end)) * 1000 * 1000);
        }
    }

    /**********************************************************************************************
     * ITEM_BID Generator
     **********************************************************************************************/
    protected class ItemBidGenerator extends SubTableGenerator<ItemInfo> {

        private ItemInfo.Bid bid = null;
        private float currentBidPriceAdvanceStep;
        private long currentCreateDateAdvanceStep;
        private boolean new_item;
        
        public ItemBidGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_BID,
                  AuctionMarkConstants.TABLENAME_ITEM);
        }
        @Override
        public short getElementCounter(ItemInfo itemInfo) {
            return (itemInfo.numBids);
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;
            assert(itemInfo.numBids > 0);
            
            UserId bidderId = null;
            
            // Figure out the UserId for the person bidding on this item now
            if (this.new_item) {
                // If this is a new item and there is more than one bid, then
                // we'll choose the bidder's UserId at random.
                // If there is only one bid, then it will have to be the last bidder
                bidderId = (itemInfo.numBids == 1 ? itemInfo.lastBidderId :
                                                    profile.getRandomBuyerId(itemInfo.sellerId));
                TimestampType endDate;
                if (itemInfo.stillAvailable) {
                    endDate = new TimestampType();
                } else {
                    endDate = itemInfo.endDate;
                }
                this.currentCreateDateAdvanceStep = (endDate.getTime() - itemInfo.startDate.getTime()) / (remaining + 1);
                this.currentBidPriceAdvanceStep = itemInfo.initialPrice * AuctionMarkConstants.ITEM_BID_PERCENT_STEP;
            }
            // The last bid must always be the item's lastBidderId
            else if (remaining == 0) {
                bidderId = itemInfo.lastBidderId; 
            }
            // The first bid for a two-bid item must always be different than the lastBidderId
            else if (itemInfo.numBids == 2) {
                assert(remaining == 1);
                bidderId = profile.getRandomBuyerId(itemInfo.lastBidderId, itemInfo.sellerId);
            } 
            // Since there are multiple bids, we want randomly select one based on the previous bidders
            // We will get the histogram of bidders so that we are more likely to select
            // an existing bidder rather than a completely random one
            else {
                assert(this.bid != null);
                Histogram<UserId> bidderHistogram = itemInfo.getBidderHistogram();
                bidderId = profile.getRandomBuyerId(bidderHistogram, this.bid.bidderId, itemInfo.sellerId);
            }
            assert(bidderId != null);

            float last_bid = (this.new_item ? itemInfo.initialPrice : this.bid.maxBid);
            this.bid = itemInfo.getNextBid(this.count, bidderId);
            this.bid.createDate = new TimestampType(itemInfo.startDate.getTime() + this.currentCreateDateAdvanceStep);
            this.bid.updateDate = this.bid.createDate; 
            
            if (remaining == 0) {
                this.bid.maxBid = itemInfo.currentPrice;
                if (itemInfo.purchaseDate != null) {
                    assert(itemInfo.getBidCount() == itemInfo.numBids) : String.format("%d != %d\n%s", itemInfo.getBidCount(), itemInfo.numBids, itemInfo);
                    profile.addWaitForPurchaseItemId(itemInfo.id);
                }
            } else {
                this.bid.maxBid = last_bid + this.currentBidPriceAdvanceStep;
            }
            
            // IB_ID
            this.row[col++] = new Long(this.bid.id);
            // IB_I_ID
            this.row[col++] = itemInfo.id;
            // IB_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IB_BUYER_ID
            this.row[col++] = this.bid.bidderId;
            // IB_BID
            this.row[col++] = this.bid.maxBid - (remaining > 0 ? (this.currentBidPriceAdvanceStep/2.0f) : 0);
            // IB_MAX_BID
            this.row[col++] = this.bid.maxBid;
            // IB_CREATED
            this.row[col++] = this.bid.createDate;
            // IB_UPDATED
            this.row[col++] = this.bid.updateDate;

            if (remaining == 0) this.updateSubTableGenerators(itemInfo);
            return (col);
        }
        @Override
        protected void newElementCallback(ItemInfo itemInfo) {
            this.new_item = true;
            this.bid = null;
        }
    }

    /**********************************************************************************************
     * ITEM_MAX_BID Generator
     **********************************************************************************************/
    protected class ItemMaxBidGenerator extends SubTableGenerator<ItemInfo> {

        public ItemMaxBidGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID,
                AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(ItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 ? 1 : 0);
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;
            ItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : "No bids?\n" + itemInfo;

            // IMB_I_ID
            this.row[col++] = itemInfo.id;
            // IMB_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IMB_IB_ID
            this.row[col++] = bid.id;
            // IMB_IB_I_ID
            this.row[col++] = itemInfo.id;
            // IMB_IB_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IMB_CREATED
            this.row[col++] = bid.createDate;
            // IMB_UPDATED
            this.row[col++] = bid.updateDate;

            if (remaining == 0) this.updateSubTableGenerators(itemInfo);
            return (col);
        }
    }

    /**********************************************************************************************
     * ITEM_PURCHASE Generator
     **********************************************************************************************/
    protected class ItemPurchaseGenerator extends SubTableGenerator<ItemInfo> {

        public ItemPurchaseGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_PURCHASE,
                  AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(ItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 && itemInfo.purchaseDate != null ? 1 : 0);
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;
            ItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : itemInfo;
            
            // IP_ID
            this.row[col++] = this.count;
            // IP_IB_ID
            this.row[col++] = bid.id;
            // IP_IB_I_ID
            this.row[col++] = itemInfo.id;
            // IP_IB_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IP_DATE
            this.row[col++] = itemInfo.purchaseDate;

            if (rng.number(1, 100) <= AuctionMarkConstants.PROB_PURCHASE_BUYER_LEAVES_FEEDBACK) {
                bid.buyer_feedback = true;
            }
            if (rng.number(1, 100) <= AuctionMarkConstants.PROB_PURCHASE_SELLER_LEAVES_FEEDBACK) {
                bid.seller_feedback = true;
            }
            
            if (remaining == 0) this.updateSubTableGenerators(bid);
            return (col);
        }
    } // END CLASS
    
    /**********************************************************************************************
     * USER_FEEDBACK Generator
     **********************************************************************************************/
    protected class UserFeedbackGenerator extends SubTableGenerator<ItemInfo.Bid> {

        public UserFeedbackGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_FEEDBACK,
                  AuctionMarkConstants.TABLENAME_ITEM_PURCHASE);
        }

        @Override
        protected short getElementCounter(Bid bid) {
            return (short)((bid.buyer_feedback ? 1 : 0) + (bid.seller_feedback ? 1 : 0));
        }
        @Override
        protected int populateRow(ItemInfo.Bid bid, short remaining) {
            int col = 0;

            boolean is_buyer = false;
            if (bid.buyer_feedback && bid.seller_feedback == false) {
                is_buyer = true;
            } else if (bid.seller_feedback && bid.buyer_feedback == false) {
                is_buyer = false;
            } else if (remaining > 1) {
                is_buyer = true;
            }
            ItemInfo itemInfo = bid.getItemInfo();
            
            // UF_ID
            this.row[col++] = this.count;
            // UF_I_ID
            this.row[col++] = itemInfo.id;
            // UF_I_U_ID
            this.row[col++] = itemInfo.sellerId;
            // UF_TO_ID
            this.row[col++] = (is_buyer ? bid.bidderId : itemInfo.sellerId);
            // UF_FROM_ID
            this.row[col++] = (is_buyer ? itemInfo.sellerId : bid.bidderId);
            // UF_RATING
            this.row[col++] = 1; // TODO
            // UF_DATE
            this.row[col++] = new TimestampType(); // Does this matter?

            return (col);
        }
    }

    /**********************************************************************************************
     * USER_ITEM Generator
     **********************************************************************************************/
    protected class UserItemGenerator extends SubTableGenerator<ItemInfo> {

        public UserItemGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_ITEM,
                  AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(ItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 && itemInfo.purchaseDate != null ? 1 : 0);
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;
            ItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : itemInfo;
            
            // UI_U_ID
            this.row[col++] = bid.bidderId;
            // UI_I_ID
            this.row[col++] = itemInfo.id;
            // UI_I_U_ID
            this.row[col++] = itemInfo.sellerId;
            // UI_IP_ID
            this.row[col++] = null;
            // UI_IP_IB_ID
            this.row[col++] = null;
            // UI_IP_IB_I_ID
            this.row[col++] = null;
            // UI_IP_IB_U_ID
            this.row[col++] = null;
            // UI_CREATED
            this.row[col++] = itemInfo.endDate;
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * USER_WATCH Generator
     **********************************************************************************************/
    protected class UserWatchGenerator extends SubTableGenerator<ItemInfo> {

        public UserWatchGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_WATCH,
                  AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(ItemInfo itemInfo) {
            return (itemInfo.numWatches);
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;
            
            // Make it more likely that a user that has bid on an item is watching it
            Histogram<UserId> bidderHistogram = itemInfo.getBidderHistogram();
            UserId buyerId = null;
            try {
                profile.getRandomBuyerId(bidderHistogram, itemInfo.sellerId);
            } catch (NullPointerException ex) {
                LOG.error("Busted Bidder Histogram:\n" + bidderHistogram);
                throw ex;
            }
            
            // UW_U_ID
            this.row[col++] = buyerId;
            // UW_I_ID
            this.row[col++] = itemInfo.id;
            // UW_I_U_ID
            this.row[col++] = itemInfo.sellerId;
            // UW_CREATED
            TimestampType endDate = (itemInfo.stillAvailable ? new TimestampType() : itemInfo.endDate);
            this.row[col++] = this.getRandomCommentDate(itemInfo.startDate, endDate);

            return (col);
        }
        private TimestampType getRandomCommentDate(TimestampType startDate, TimestampType endDate) {
            long start = (startDate.getTime() / 1000000);
            long end = (endDate.getTime() / 1000000);
            return new TimestampType((AuctionMarkLoader.this.rng.number(start, end) * 1000 * 1000));
        }
    } // END CLASS
} // END CLASS