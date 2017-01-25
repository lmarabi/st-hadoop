package edu.umn.cs.sthadoop.operations;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;

/***
 * Implementation of spatiotemporal range query. 
 * @author louai Alarabi
 *
 */

public class STRangeQuery {

	public STRangeQuery( Path indexesPath, OperationsParams params) {
		// TODO Auto-generated constructor stub
	}

	private static void printUsage() {
	    System.out.println("Runs a spatio-temporal aggregate query on indexed MODIS data");
	    System.out.println("Parameters: (* marks required parameters)");
	    System.out.println("<input file> - (*) Path to input file");
	    System.out.println("<output file> -  Path to input file");
	    System.out.println("shape:<STPoint> - (*) Type of shapes stored in input file");
	    System.out.println("rect:<x1,y1,x2,y2> - Spatial query range");
	    System.out.println("time:<date1,date2> - Temporal query range. "
	        + "Format of each date is yyyy-mm-dd HH:MM:SS");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws Exception {
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args), false);
		// Check input
		if (!params.checkInput()) {
			printUsage();
			System.exit(1);
		}

		if (params.get("rect") == null) {
			System.err.println("Spatial range missing");
			printUsage();
			System.exit(1);
		}

		if (params.get("time") == null) {
			System.err.println("Temporal range missing");
			printUsage();
			System.exit(1);
		}
		// code to handle the spatiotemporal range query.
		// First check the range query.
		// Decide on which level you will query from
		// use spatialhadoop range query to report the answer.
	}

}
