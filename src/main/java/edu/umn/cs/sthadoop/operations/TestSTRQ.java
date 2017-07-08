package edu.umn.cs.sthadoop.operations;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.operations.RangeQuery;
import edu.umn.cs.sthadoop.core.QueryPlanner;
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

public class TestSTRQ {
	private static final Log LOG = LogFactory.getLog(TestSTRQ.class);
	private Path indexPath; 
	private Path outputPath; 
	private String fromTime;
	private String toTime;


	public TestSTRQ(OperationsParams params) throws Exception {
		// code to handle the spatiotemporal range query.
		 indexPath = params.getInputPath();
		 outputPath = params.getOutputPath();
		 String fromto = params.get("interval");
		 if(fromto.contains(",")){
			 // query temporal range different date
			 String[] time = fromto.split(",");
			 fromTime = time[0];
			 toTime = time[1];
			 QueryPlanner plan = new QueryPlanner(params);
			 List<Path> slices = plan.getQueryPlan(fromTime, toTime); 
			 for(Path indexedslice : slices){
				 System.out.println(indexedslice.getName().toString());
				 //RangeQuery.rangeQueryMapReduce(indexedslice, outputPath, params);
				 
			 }
			 
		 }else{
			 // query temporal range same date
		 }
		 
		// First check the range query.
		// Decide on which level you will query from
		// use spatialhadoop range query to report the answer.
	}
	
	

	private static void printUsage() {
		System.out.println("Runs a spatio-temporal range query on indexed data");
		System.out.println("Parameters: (* marks required parameters)");
		System.out.println("<input file> - (*) Path to input file");
		System.out.println("<output file> -  Path to input file");
		System.out.println("shape:<STPoint> - (*) Type of shapes stored in input file");
		System.out.println("rect:<x1,y1,x2,y2> - Spatial query range");
		System.out
				.println("interval:<date1,date2> - Temporal query range. " + "Format of each date is yyyy-mm-dd HH:MM:SS");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws Exception {
		args = new String[6];
		args[0] = "/home/louai/nyc-taxi/yellowIndex";
		args[1] = "shape:edu.umn.cs.sthadoop.core.STPoint";
		args[2] = "rect:-74.98451232910156,35.04014587402344,-73.97936248779295,41.49399566650391";
		args[3] = "interval:2015-01-01,2015-01-03";
		args[4] = "-overwrite";
		args[5	] = "-no-local";
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args), false);
		// Check input
		 final Path[] paths = params.getPaths();
		    if (paths.length <= 1 && !params.checkInput()) {
		      printUsage();
		      System.exit(1);
		    }
		    if (paths.length >= 2 && !params.checkInputOutput()) {
		      printUsage();
		      System.exit(1);
		    }
		    if (params.get("rect") == null) {
		      System.err.println("You must provide a query range");
		      printUsage();
		      System.exit(1);
		    }

		if (params.get("interval") == null) {
			System.err.println("Temporal range missing");
			printUsage();
			System.exit(1);
		}
		
		TextSerializable inObj = params.getShape("shape");
		if (!(inObj instanceof STPoint)) {
			LOG.error("Shape is not instance of STPoint");
			printUsage();
			System.exit(1);
		} else {
			// check the validation of the range query
		    long t1 = System.currentTimeMillis();
		    TestSTRQ query = new TestSTRQ(params);
		    int result =0;
		    long t2 = System.currentTimeMillis();
		    System.out.println("STRQ finished in "+(t2-t1)+" millis");
		}
		


	}

}
