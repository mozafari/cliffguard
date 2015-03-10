package edu.umich.robustopt.workloads;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import edu.umich.robustopt.common.Randomness;


public class WideTableDataGenerator {

	public static void createSchemaFile(int numberOfColumns, String schemaFilename) throws IOException {
/*		CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                N_NAME       CHAR(25) NOT NULL,
                N_REGIONKEY  INTEGER NOT NULL,
                N_COMMENT    VARCHAR(152));
*/
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(new File(schemaFilename)));
		StringBuilder buf = new StringBuilder();
		buf.append(("CREATE TABLE WIDE" + numberOfColumns + " (\n"));
		for (int column=1; column<=numberOfColumns-1; ++column) {
			buf.append("\tcol"+column+"\t INTEGER NOT NULL,\n");
		}
		buf.append("\tcol"+numberOfColumns+"\t INTEGER NOT NULL);\n");
		outputFile.write(buf.toString());
		outputFile.close();
		System.out.println("Schema file written to " + schemaFilename);
	}
	
	public static void createCsvFile(int numberOfColumns, int columnCardinality, int numberOfRows, String dataFilename) throws IOException {
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(new File(dataFilename)));
		for (int row=0; row<numberOfRows; ++row) {
			StringBuilder buf = new StringBuilder();
			for (int column=0; column<numberOfColumns; ++column) {
				int value = Randomness.randGen.nextInt(columnCardinality);
				buf.append(value + "|");
			}
			outputFile.write(buf.toString() + "\n");
			if (row % 1000000 == 0)
				System.out.println("written: "+row);
				//outputFile.flush();
		}
		outputFile.close();
		System.out.println("Data file written to " + dataFilename);
	}
	
	public static void createLoadScript(int numberOfColumns, String scriptName) throws IOException {
//		COPY CUSTOMER FROM '/home/dbadmin/dataset_tpch/customer.tbl' delimiter '|';
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(new File(scriptName)));
		outputFile.write("COPY WIDE" + numberOfColumns + " FROM '/home/dbadmin/robust-opt/dataset_wide/wide" + numberOfColumns  +".tbl' delimiter '|';\n");
		outputFile.close();
		System.out.println("Script file written to " + scriptName);
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length != 4) {
			System.err.println("Invalid parameters.\nUsage: WideTableGenerator noColumns columnCardinality noRows outputFilename");
			return;
		}
		int numberOfColumns = Integer.parseInt(args[0]);
		int columnCardinality = Integer.parseInt(args[1]);
		int numberOfRows = Integer.parseInt(args[2]);
		String outputFilename = args[3];
		
		createCsvFile(numberOfColumns, columnCardinality, numberOfRows, outputFilename+".tbl");
		createSchemaFile(numberOfColumns, outputFilename + "_schema_def");
		createLoadScript(numberOfColumns, outputFilename + ".sh");
	}

}
