package com.example.benchmark.abc;
 
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.BenchmarkComponent;
import com.example.benchmark.abc.procedures.*;
 
public class ABCProjectBuilder extends AbstractProjectBuilder {
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = ABCClient.class;
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = ABCLoader.class;
 
    public static final Class<?> PROCEDURES[] = new Class<?>[] {
        GetData.class,
    };
    public static final String PARTITIONING[][] = new String[][] {
        // { "TABLE NAME", "PARTITIONING COLUMN NAME" }
        {"TABLEA", "A_ID"},
        {"TABLEB", "B_A_ID"},
    };
 
    public ABCProjectBuilder() {
        super("abc", ABCProjectBuilder.class, PROCEDURES, PARTITIONING);
 
        // Create a single-statement stored procedure named 'DeleteData'
        addStmtProcedure("DeleteData", "DELETE FROM TABLEA WHERE A_ID < ?");
    }
}