package edu.umich.robustopt.clustering;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;


public class QueryClusteringVerbatim extends QueryClustering {

	@Override
	public void cluster(String input_query_log, String clustering_output) throws Exception {
		if (1==1)
			throw new Exception("This function cannot be called, since the input has to be made unspaced and sorted!");
		
		BufferedReader bufferedReader = null;
		try {
			bufferedReader = new BufferedReader(new FileReader(input_query_log));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			return;
		}
		
		ArrayList<Integer> frequencies = new ArrayList<Integer>();
		try {
			String prevLine = null;
			String line; 
			int lineNumber = 0;
			int currentFreq = 1;
			while ((line = bufferedReader.readLine())!=null) {
				++lineNumber;
				if (lineNumber>1) {
	 				if (line.toLowerCase().equals(prevLine.toLowerCase())) 
						++ currentFreq;
					else {
						frequencies.add(currentFreq);
						currentFreq = 1;
					}
				}
				prevLine = line;				
			}
			bufferedReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		PrintWriter printWriter = null;
		try {
			printWriter = new PrintWriter(new FileWriter(clustering_output));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		for (int freq: frequencies) {
			printWriter.println(freq);
		}
		printWriter.close();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
