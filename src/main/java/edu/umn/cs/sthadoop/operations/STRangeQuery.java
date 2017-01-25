package edu.umn.cs.sthadoop.operations;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.sthadoop.core.STPoint;
import edu.umn.cs.sthadoop.core.TimeFormatST;
import edu.umn.cs.sthadoop.core.TimeFormatST.TimeFormatEnum;
import edu.umn.cs.sthadoop.indexing.STIndexManager;


/***
 * Implementation of spatiotemporal range query.
 * 
 * @author louai Alarabi
 *
 */

public class STRangeQuery {
	private static final Log LOG = LogFactory.getLog(STRangeQuery.class);
	private Path indexPath; 
	private Path outputPath; 
	private TimeFormatST fromTime;
	private TimeFormatST toTime;


	public STRangeQuery(OperationsParams params) {
		// code to handle the spatiotemporal range query.
		 indexPath = params.getInputPath();
		 outputPath = params.getOutputPath();
		 String fromto = params.get("time");
		 if(fromto.contains(",")){
			 // query temporal range different date
			 String[] time = fromto.split(",");
			 
		 }else{
			 // query temporal range same date
		 }
		 
		// First check the range query.
		// Decide on which level you will query from
		// use spatialhadoop range query to report the answer.
	}
	
	public int RangeQuery(){
		int result = 0; 
		return result;
	}
	

	private static void printUsage() {
		System.out.println("Runs a spatio-temporal aggregate query on indexed MODIS data");
		System.out.println("Parameters: (* marks required parameters)");
		System.out.println("<input file> - (*) Path to input file");
		System.out.println("<output file> -  Path to input file");
		System.out.println("shape:<STPoint> - (*) Type of shapes stored in input file");
		System.out.println("rect:<x1,y1,x2,y2> - Spatial query range");
		System.out
				.println("time:<date1,date2> - Temporal query range. " + "Format of each date is yyyy-mm-dd HH:MM:SS");
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
		
		TextSerializable inObj = params.getShape("shape");
		if (!(inObj instanceof STPoint)) {
			LOG.error("Shape is not instance of STPoint");
			return;
		} else {
			// check the validation of the range query
		    long t1 = System.currentTimeMillis();
		    int result =0;
		    long t2 = System.currentTimeMillis();
			System.out.println("Final Result: "+result);
		    System.out.println("Aggregate query finished in "+(t2-t1)+" millis");
		}
		


	}

}
