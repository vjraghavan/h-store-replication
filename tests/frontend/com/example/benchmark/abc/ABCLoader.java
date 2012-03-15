package com.example.benchmark.abc;
 
import java.util.Iterator;

import org.voltdb.VoltTable;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Table;

import edu.brown.benchmark.BenchmarkComponent;
import edu.brown.catalog.CatalogUtil;
 
public class ABCLoader extends BenchmarkComponent {
 
    public static void main(String args[]) throws Exception {
        BenchmarkComponent.main(ABCLoader.class, args, true);
    }
 
    public ABCLoader(String[] args) {
        super(args);
        for (String key : m_extraParams.keySet()) {
            // TODO: Retrieve extra configuration parameters
        } // FOR
    }
 
    @Override
    public void runLoop() {
        // The catalog contains all the information about the database (e.g., tables, columns, indexes)
        // It is loaded from the benchmark's project JAR file
        Catalog catalog = this.getCatalog();
 
        
        // Iterate over all of the Table handles in the catalog and generate
        // tuples to upload into the database.
        
        Iterator<Table> iter = CatalogUtil.getDatabase(catalog).getTables().iterator();
        Table tableA = iter.next();
        Table tableB = iter.next();
        VoltTable voltTableA = CatalogUtil.getVoltTable(tableA);
        VoltTable voltTableB = CatalogUtil.getVoltTable(tableB);
        
        if (tableA.getName().equals("TABLEA")) {
        	populateVoltTableA(voltTableA);
        	populateVoltTableB(voltTableB);
        } else {
        	populateVoltTableA(voltTableB);
        	populateVoltTableB(voltTableA);
        }
        
        this.loadVoltTable(tableA.getName(), voltTableA);
        this.loadVoltTable(tableB.getName(), voltTableB);
    }
    
    private void populateVoltTableB(VoltTable voltTable) {
    	Object[] row = new Object[]{1, 1, "1"};
		voltTable.addRow(row);
	}

	private void populateVoltTableA(VoltTable voltTable) {
		Object[] row = new Object[]{1, "1"};
		voltTable.addRow(row);
		
		row = new Object[]{2, "2"};
		voltTable.addRow(row);
		
		row = new Object[]{3, "3"};
		voltTable.addRow(row);
		
		row = new Object[]{4, "4"};
		voltTable.addRow(row);
	}

	@Override
    public String[] getTransactionDisplayNames() {
        // IGNORE: Only needed for Client
        return new String[] {};
    }
}