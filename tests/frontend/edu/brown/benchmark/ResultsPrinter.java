/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package edu.brown.benchmark;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import edu.brown.benchmark.BenchmarkResults.EntityResult;
import edu.brown.benchmark.BenchmarkResults.FinalResult;
import edu.brown.benchmark.BenchmarkResults.Result;
import edu.brown.statistics.Histogram;
import edu.brown.utils.StringUtil;
import edu.brown.utils.TableUtil;

public class ResultsPrinter implements BenchmarkController.BenchmarkInterest {

    private static final String COL_FORMATS[] = {
        "%23s:",
        "%10d total",
        "(%5.1f%%)",
        "%8.2f txn/s",
        "%10.2f txn/m",
    };
    
    protected final boolean output_clients;
    protected final boolean output_basepartitions;
    
    public ResultsPrinter(boolean output_clients, boolean output_basepartitions) {
        this.output_clients = output_clients;
        this.output_basepartitions = output_basepartitions;
    }
    
    @Override
    public String formatFinalResults(BenchmarkResults results) {
        StringBuilder sb = new StringBuilder();
        FinalResult fr = new FinalResult(results);
        
        final int width = 100; 
        sb.append(String.format("\n%s\n", StringUtil.header("BENCHMARK RESULTS", "=", width)));
        sb.append(String.format("Time: %d ms\n", fr.getDuration()));
        sb.append(String.format("Total transactions: %d\n", fr.getTotalTxnCount()));
        sb.append(String.format("Transactions per second: %.2f  [min:%.2f / max:%.2f]\n\n",
                                fr.getTotalTxnPerSecond(), fr.getMinTxnPerSecond(), fr.getMaxTxnPerSecond()));
        
        Collection<String> txnNames = fr.getTransactionNames();
        Collection<String> clientNames = fr.getClientNames();
        int num_rows = txnNames.size() + (this.output_clients ? clientNames.size() + 1 : 0);
        Object rows[][] = new String[num_rows][COL_FORMATS.length];
        int row_idx = 0;
        
        for (String txnName : txnNames) {
            EntityResult er = fr.getTransactionResult(txnName);
            assert(er != null);
            int col_idx = 0;
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], txnName);
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnCount());
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnPercentage());
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnPerMilli());
            rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnPerSecond());
            row_idx++;
        } // FOR

        if (output_clients) {
            rows[row_idx][0] = "\nBreakdown by client:";
            for (int i = 1; i < COL_FORMATS.length; i++) {
                rows[row_idx][i] = "";
            } // FOR
            row_idx++;
            
            for (String clientName : clientNames) {
                EntityResult er = fr.getClientResult(clientName);
                assert(er != null);
                int col_idx = 0;
                rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], clientName);
                rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnCount());
                rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnPercentage());
                rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnPerMilli());
                rows[row_idx][col_idx++] = String.format(COL_FORMATS[col_idx-1], er.getTxnPerSecond());
                row_idx++;
            } // FOR
        }
        sb.append(TableUtil.table(rows));
        sb.append(String.format("\n%s\n", StringUtil.repeat("=", width)));
        
        if (output_basepartitions) {
            sb.append("Transaction Base Partitions:\n");
            Histogram<Integer> h = results.getBasePartitions();
            Map<Integer, String> labels = new HashMap<Integer, String>();
            for (Integer p : h.values()) {
                labels.put(p, String.format("Partition %02d", p));
            } // FOR
            h.setDebugLabels(labels);
            sb.append(h.toString((int)(width * 0.75)));
            sb.append(String.format("\n%s\n", StringUtil.repeat("=", width)));
        }
        
        return (sb.toString());
    }
    
    @Override
    public void benchmarkHasUpdated(BenchmarkResults results) {

        long totalTxnCount = 0;
        for (String client : results.getClientNames()) {
            for (String txn : results.getTransactionNames()) {
                Result[] rs = results.getResultsForClientAndTransaction(client, txn);
                for (Result r : rs)
                    totalTxnCount += r.transactionCount;
            }
        }

        long txnDelta = 0;
        for (String client : results.getClientNames()) {
            for (String txn : results.getTransactionNames()) {
                Result[] rs = results.getResultsForClientAndTransaction(client, txn);
                Result r = rs[rs.length - 1];
                txnDelta += r.transactionCount;
            }
        }

        int pollIndex = results.getCompletedIntervalCount();
        long duration = results.getTotalDuration();
        long pollCount = duration / results.getIntervalDuration();
        long currentTime = pollIndex * results.getIntervalDuration();

        System.out.printf("\nAt time %d out of %d (%d%%):\n", currentTime, duration, currentTime * 100 / duration);
        System.out.printf("  In the past %d ms:\n", duration / pollCount);
        System.out.printf("    Completed %d txns at a rate of %.2f txns/s\n",
                txnDelta,
                txnDelta / (double)(results.getIntervalDuration()) * 1000.0);
        System.out.printf("  Since the benchmark began:\n");
        System.out.printf("    Completed %d txns at a rate of %.2f txns/s\n",
                totalTxnCount,
                totalTxnCount / (double)(pollIndex * results.getIntervalDuration()) * 1000.0);


//        if ((pollIndex * results.getIntervalDuration()) >= duration) {
//            // print the final results
//            System.out.println(this.formatFinalResults(results));
//        }

        System.out.flush();
    }
}
