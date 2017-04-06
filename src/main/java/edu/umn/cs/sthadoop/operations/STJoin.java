/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.sthadoop.operations;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.sthadoop.core.STPoint;

/**
 * Implementation of Spatio-temporal Join, takes two dataset and joins them
 * based on Spatial and temporal predicates. For example, Join birds and human
 * at area A during time interval T.
 * 
 * @author Louai Alarabi
 *
 */
public class STJoin {

	/** Class logger */
	private static final Log LOG = LogFactory.getLog(STJoin.class);

	static class STJoinMap extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

		private GridInfo gridInfo;
		private IntWritable cellId = new IntWritable();

		@Override
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			STPoint shape = new STPoint();
			shape.fromText(value);
			LOG.info("<Log>---->  I'm in mapper: " + shape.toString());
			System.out.println("<println>-------> I'm in mapper: " + shape.toString());
			java.awt.Rectangle cells = gridInfo.getOverlappingCells(shape.getMBR());

			for (int col = cells.x; col < cells.x + cells.width; col++) {
				for (int row = cells.y; row < cells.y + cells.height; row++) {
					cellId.set(row * gridInfo.columns + col + 1);
					output.collect(new Text(cellId.toString()), new Text(shape.toString()));
				}
			}
		}
	}

	static class STJoinReduce extends MapReduceBase implements 
	Reducer<Text, Text, Text, Text> {		/** List of cells used by the reducer */
		private GridInfo grid;

		@Override
		public void configure(JobConf job) {
			super.configure(job);
			grid = (GridInfo) OperationsParams.getShape(job, "PartitionGrid");
		}

		@Override
		public void reduce(Text cellId, Iterator<Text> values, final OutputCollector<Text,Text> output,
				Reporter reporter) throws IOException {
			// Extract CellInfo (MBR) for duplicate avoidance checking
			LOG.info("<Log>---->  I'm in reducer: ");
			System.out.println("<println>-------> I'm in reducer: ");
			while(values.hasNext()){
				output.collect(new Text(cellId.toString()), values.next());
				LOG.info("<Log>---->  I'm in reducer: ");
				System.out.println("<println>-------> I'm in reducer: ");
			}
			
//			final CellInfo cellInfo = grid.getCell(cellId.get());
//
//			Vector<S> shapes = new Vector<S>();
//
//			while (values.hasNext()) {
//				S s = values.next();
//				shapes.add((S) s.clone());
//			}
//
//			SpatialAlgorithms.SelfJoin_planeSweep(shapes.toArray(new Shape[shapes.size()]), true,
//					new OutputCollector<Shape, Shape>() {
//
//						@Override
//						public void collect(Shape r, Shape s) throws IOException {
//							// Perform a reference point duplicate avoidance
//							// technique
//							Rectangle intersectionMBR = r.getMBR().getIntersection(s.getMBR());
//							// Error: intersectionMBR may be null.
//							if (intersectionMBR != null) {
//								if (cellInfo.contains(intersectionMBR.x1, intersectionMBR.y1)) {
//									// Report to the reduce result collector
//									output.collect((S) r, (S) s);
//								}
//							}
//						}
//					}, new Progressable.ReporterProgressable(reporter));
		}
	}

	/**
	 * 
	 * @param inputPath
	 * @param outputPath
	 * @param params
	 * @return
	 * @throws IOException
	 * @throws Exception
	 * @throws InterruptedException
	 */
	private static long stJoin(Path inputPath, Path outputPath, OperationsParams params)
			throws IOException, Exception, InterruptedException {
		JobConf conf = new JobConf(new Configuration(), STJoin.class);
		FileSystem outfs = outputPath.getFileSystem(conf);
		outfs.delete(outputPath, true);
		conf.setJobName("STJoin Query");
		conf.setOutputKeyClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputValueClass(Text.class);
		// Mapper settings
		conf.setMapperClass(STJoinMap.class);
		conf.setReducerClass(STJoinReduce.class);
		conf.setCombinerClass(STJoinReduce.class);
		//conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);
		// grid partition.
		GridInfo gridInfo = new GridInfo(-Double.MAX_VALUE, -Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE);
		gridInfo.calculateCellDimensions(20);
		OperationsParams.setShape(conf, "PartitionGrid", gridInfo);
		JobClient.runJob(conf).waitForCompletion();
		;
		System.out.println("Job1 finish");
		return 0;
	}
	
	

	private static void printUsage() {
		System.out.println("Runs a spatio-temporal range query on indexed data");
		System.out.println("Parameters: (* marks required parameters)");
		System.out.println("<input file> - (*) Path to input file");
		System.out.println("<output file> -  Path to input file");
		System.out.println("shape:<STPoint> - (*) Type of shapes stored in input file");
		System.out.println("rect:<x1,y1,x2,y2> - Spatial query range");
		System.out.println("interval:<date1,date2> - Temporal query range. " + "Format of each date is yyyy-mm-dd");
		System.out.println("time:[day,week,month,year] -  Time Format");
		System.out.println("-overwrite - Overwrite output file without notice");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

//		 args = new String[8];
//		 args[0] = "/home/louai/nyc-taxi/yellowIndex";
//		 args[1] = "/home/louai/nyc-taxi/humanIndex";
//		 args[2] = "/home/louai/nyc-taxi/resultSTJoin";
//		 args[3] = "shape:edu.umn.cs.sthadoop.core.STPoint";
//		 args[4] =
//		 "rect:-74.98451232910156,35.04014587402344,-73.97936248779295,41.49399566650391";
//		 args[5] = "interval:2015-01-01,2015-01-02";
//		 args[6] = "-overwrite";
//		 args[7] = "-no-local";

		OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		Path[] allFiles = params.getPaths();
		if (allFiles.length < 2) {
			System.err.println("This operation requires at least two input files");
			printUsage();
			System.exit(1);
		}
		if (allFiles.length == 2 && !params.checkInput()) {
			// One of the input files does not exist
			printUsage();
			System.exit(1);
		}
		if (allFiles.length > 2 && !params.checkInputOutput()) {
			printUsage();
			System.exit(1);
		}

		Path[] inputPaths = allFiles.length == 2 ? allFiles : params.getInputPaths();
		Path outputPath = allFiles.length == 2 ? null : params.getOutputPath();

		// Query from the dataset.
		for (Path input : inputPaths) {
			args = new String[7];
			args[0] = input.toString();
			args[1] = outputPath.getParent().toString() + "candidatebuckets/" + input.getName();
			args[2] = "shape:" + params.get("shape");
			args[3] = "rect:" + params.get("rect");
			args[4] = "interval:" + params.get("interval");
			args[5] = "-overwrite";
			args[6] = "-no-local";
			for (String x : args)
				System.out.println(x);
//			STRangeQuery.main(args);
			System.out.println("done with the STQuery from: " + input.toString() + "\n" + "candidate:" + args[1]);

		}
		// invoke the map-hash and reduce-join .
	    FileSystem fs = outputPath.getFileSystem(new Configuration());
	    Path inputstjoin;
	    if(fs.exists(new Path(outputPath.getParent().toString() + "candidatebuckets/"))){
	    	inputstjoin = new Path(outputPath.getParent().toString() + "candidatebuckets/*/*/*");
	    }else{
	    	inputstjoin = new Path(outputPath.getParent().toString() + "/candidatebuckets/*/*/*");
	    }
		long t1 = System.currentTimeMillis();
		long resultSize = stJoin(inputstjoin, outputPath, params);
		long t2 = System.currentTimeMillis();
		System.out.println("Total time: " + (t2 - t1) + " millis");
		System.out.println("Result size: " + resultSize);
	}

}
