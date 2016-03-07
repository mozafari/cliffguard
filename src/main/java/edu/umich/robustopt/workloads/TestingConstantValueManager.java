package edu.umich.robustopt.workloads;

import java.sql.Connection;
import java.util.HashMap;


import edu.umich.robustopt.dblogin.DatabaseLoginConfiguration;
import edu.umich.robustopt.staticanalysis.ColumnDescriptor;
import edu.umich.robustopt.vertica.VerticaDatabaseLoginConfiguration;

public class TestingConstantValueManager extends ConstantValueManager {

	public TestingConstantValueManager() throws Exception {
		super("noname", new HashMap<String, ValueDistribution>(), 1.0, null, null);
	}

	@Override
	public ValueDistribution getColumnDistribution(ColumnDescriptor cd) {
		return ValueDistribution.DummyDistribution;
	}
	
	public static void main(String[] args) {
		try {
			TestingConstantValueManager cvm = new TestingConstantValueManager();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
