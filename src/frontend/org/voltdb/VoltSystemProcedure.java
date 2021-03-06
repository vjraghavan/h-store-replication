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

package org.voltdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;

/**
 * System procedures extend VoltSystemProcedure and use its utility methods to
 * create work in the system. This functionality is not available to standard
 * user procedures (which extend VoltProcedure).
 */
public abstract class VoltSystemProcedure extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(VoltSystemProcedure.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /** Standard column type for host/partition/site id columns */
    protected static VoltType CTYPE_ID = VoltType.INTEGER;

    /** Standard column name for a host id column */
    protected static String CNAME_HOST_ID = "HOST_ID";

    /** Standard column name for a site id column */
    protected static String CNAME_SITE_ID = "SITE_ID";

    /** Standard column name for a partition id column */
    protected static String CNAME_PARTITION_ID = "PARTITION_ID";

    /** Standard schema for sysprocs returning a simple status table */
    public static ColumnInfo STATUS_SCHEMA =
        new ColumnInfo("STATUS", VoltType.BIGINT);   // public to fix javadoc linking warning

    /** Standard success return value for sysprocs returning STATUS_SCHEMA */
    protected static long STATUS_OK = 0L;
    
    protected Database database = null;
    protected int num_partitions;

    
    @Override
    public void globalInit(PartitionExecutor site, Procedure catalog_proc, BackendTarget eeType, HsqlBackend hsql, PartitionEstimator pEstimator) {
        super.globalInit(site, catalog_proc, eeType, hsql, pEstimator);
        this.database = CatalogUtil.getDatabase(catalog_proc);
        this.num_partitions = CatalogUtil.getNumberOfPartitions(catalog_proc);
    }

    /**
     * Utility to aggregate a list of tables sharing a schema. Common for
     * sysprocs to do this, to aggregate results.
     */
    protected VoltTable unionTables(List<VoltTable> operands) {
        VoltTable result = null;
        VoltTable vt = operands.get(0);
        if (vt != null) {
            VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[vt
                                                                        .getColumnCount()];
            for (int ii = 0; ii < vt.getColumnCount(); ii++) {
                columns[ii] = new VoltTable.ColumnInfo(vt.getColumnName(ii),
                                                       vt.getColumnType(ii));
            }
            result = new VoltTable(columns);
            for (Object table : operands) {
                vt = (VoltTable) (table);
                while (vt.advanceRow()) {
                    result.add(vt);
                }
            }
        }
        return result;
    }

    /** Bundles the data needed to describe a plan fragment. */
    public static class SynthesizedPlanFragment {
        public int destPartitionId = -1;
        public int fragmentId = -1;
        public int inputDependencyIds[] = null;
        public int outputDependencyIds[] = null;
        public ParameterSet parameters = null;
        public boolean multipartition = false;   /** true if distributes to all executable partitions */
        public boolean nonExecSites = false;     /** true if distributes once to each node */
        public boolean last_task = false;
    }

    abstract public DependencySet executePlanFragment(long txn_id,
                                                      Map<Integer,List<VoltTable>> dependencies,
                                                      int fragmentId,
                                                      ParameterSet params,
                                                      PartitionExecutor.SystemProcedureExecutionContext context);

    /**
     * Produce work units, possibly on all sites, for a list of plan fragments.
     * The final plan fragment must aggregate intermediate results and produce
     * a single output dependency. This aggregate output is returned as the result.
     *
     * @param pfs an array of synthesized plan fragments
     * @param aggregatorOutputDependencyId dependency id produced by the aggregation pf
     *        The id of the table returned as the result of this procedure.
     * @return the resulting VoltTable as a length-one array.
     */
    protected VoltTable[] executeSysProcPlanFragments(SynthesizedPlanFragment pfs[],
                                                      int aggregatorOutputDependencyId) {
        // Block until we get all of our responses.
        // We can do this because our ExecutionSite is multi-threaded
        return (executeSysProcPlanFragmentsAsync(pfs));
    }

    /**
     * Produce work units, possibly on all sites, for a list of plan fragments.
     * The final plan fragment must aggregate intermediate results and produce
     * a single output dependency. This aggregate output is returned as the result.
     *
     * @param pfs an array of synthesized plan fragments
     * @param aggregatorOutputDependencyId dependency id produced by the aggregation pf
     *        The id of the table returned as the result of this procedure.
     */
    protected final VoltTable[] executeSysProcPlanFragmentsAsync(SynthesizedPlanFragment pfs[]) {
        LOG.debug("Preparing to execute " + pfs.length + " sysproc fragments");
//        List<FragmentTaskMessage> ftasks = new ArrayList<FragmentTaskMessage>();

        List<WorkFragment> ftasks = new ArrayList<WorkFragment>();
        ParameterSet parameters[] = new ParameterSet[pfs.length];
        for (int i = 0; i < pfs.length; i++) {
            SynthesizedPlanFragment pf = pfs[i];
            // check mutually exclusive flags
            assert(!(pf.multipartition && pf.nonExecSites));
            assert (pf.parameters != null);
            // assert(pf.outputDependencyIds.length > 0) : "The DependencyId list is empty!!!";

            // serialize parameters
            parameters[i] = pf.parameters;
//            ByteString parambytes = ByteString.EMPTY; 
//            if (pf.parameters != null) {
//                FastSerializer fs = new FastSerializer();
//                try {
//                    fs.writeObject(pf.parameters);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                    assert (false);
//                }
//                parambytes = ByteString.copyFrom(fs.getBuffer());
//            }

            if (debug.get()) 
                LOG.debug(String.format("Creating SysProc FragmentTaskMessage for %s in %s",
                                       (pf.destPartitionId < 0 ? "coordinator" : "partition #" + pf.destPartitionId),
                                       this.getTransactionState()));

            WorkFragment.Builder builder = WorkFragment.newBuilder()
                                                    .setPartitionId(pf.destPartitionId)
                                                    .setReadOnly(false)
                                                    .setLastFragment(pf.last_task)
                                                    .addFragmentId(pf.fragmentId)
                                                    .addStmtIndex(i);
            
            // Input Dependencies
            boolean needs_input = false;
            WorkFragment.InputDependency.Builder inputBuilder = WorkFragment.InputDependency.newBuilder();
            for (int dep : pf.inputDependencyIds) {
                inputBuilder.addIds(dep);
                needs_input = needs_input || (dep != HStoreConstants.NULL_DEPENDENCY_ID);
            } // FOR
            builder.addInputDepId(inputBuilder.build());
            
            // Output Dependencies
            for (int dep : pf.outputDependencyIds) {
                builder.addOutputDepId(dep);
            } // FOR

            builder.setNeedsInput(needs_input);
            ftasks.add(builder.build());
            
//            FragmentTaskMessage task = new FragmentTaskMessage(
//                    this.executor.getPartitionId(),
//                    (int)pf.destPartitionId,
//                    this.getTransactionId(),
//                    -1,
//                    false,
//                    new long[] { pf.fragmentId },
//                    pf.inputDependencyIds,
//                    pf.outputDependencyIds,
//                    new ByteBuffer[] { parambytes },
//                    new int[] { 0 },
//                    pf.last_task);
//            task.setFragmentTaskType(FragmentTaskMessage.SYS_PROC_PER_PARTITION);
//            ftasks.add(task);
        } // FOR

        LocalTransaction ts = (LocalTransaction)this.getTransactionState();
        
        // HACK
        if (hstore_conf.site.txn_profiling) {
            ts.profiler.disableProfiling();
        }
        return (this.executor.dispatchWorkFragments(ts, ftasks, parameters));
    }
}
