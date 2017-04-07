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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.io.output.NullWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

	static class STJoinMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private double degree = 0.01;
		private double x1 = -180;
		private double y1 = -90;
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			if(value != null){
			STPoint shape = new STPoint();
			shape.fromText(value);
			// Hasing objects to the grid. 
			int columnID = (int)((shape.x - x1)/ degree);
			int rowID = (int)((shape.y - y1)/ degree);
			output.collect(new Text(columnID+","+rowID), new Text(shape.toString()));
			}
		}
	}

	static class STJoinReduce extends MapReduceBase implements 
	Reducer<Text, Text, Text, Text> {		
		private static final Text NullWritable = null;
		private Text pair1 = new Text();
		private Text pair2 = new Text();
		private Text joinResult = new Text();
		int distance = 0; 
		String time = "";
		int interval = 0;	
		
		
		 @Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			super.configure(job);
			String[] temp = job.get("timedistance").split(".");
			this.time = temp[1];
			this.interval = Integer.parseInt(temp[0]);
			this.distance = Integer.parseInt(job.get("spacedistance"));
		}


	/** List of cells used by the reducer */
		

		@Override
		public void reduce(Text cellId, Iterator<Text> values, final OutputCollector<Text,Text> output,
				Reporter reporter) throws IOException {
			ArrayList<STPoint> shapes = new ArrayList<STPoint>();
			STPoint shape = null;
			while(values.hasNext()){
				shape = new STPoint();
				shape.fromText(values.next());
				shapes.add(shape);
			}
			
			// find nested join result. 
			for(STPoint point : shapes){
				for(STPoint x : shapes){
					if(point.equals(x))
						continue;
					if(point.distanceTo(x) <= distance && 
							getTimeDistance(point.time, x.time, time, interval)){
						point.toText(pair1);
						x.toText(pair2);
						joinResult.set(pair1.toString()+"\t"+pair2.toString());
						output.collect(cellId, joinResult);
					}
				}
			}
			
		}
		
		
		 private boolean getTimeDistance(String time1 , String time2, String flag, int interval) {
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				boolean result = false;
				try {
					Date d1 = format.parse(time1);
					Date d2 = format.parse(time2);

					//in milliseconds
					long diff = d2.getTime() - d1.getTime();
					if(flag.equals("day")){
						if(interval <= (diff / (24 * 60 * 60 * 1000)))
							result = true;
					}else if(flag.equals("hour")){
						if(interval <= (diff / (60 * 60 * 1000) % 24))
							result = true;
					}else if(flag.equals("minute")){
						if(interval <= (diff / (60 * 1000) % 60))
							result = true;
					}else if(flag.equals("second")){
						if(interval <= (diff / 1000 % 60))
							result = true;
					}else{
						return result;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

				return result;
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
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);
		// pass params to the join map-reduce 
		conf.set("timedistance", params.get("timedistance"));
		conf.set("spacedistance", params.get("spacedistance"));
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
		System.out.println("timeDistance:[1.day,1.hour,30.minute,120.second] -  time distance degree");
		System.out.println("spaceDistance:integer -  time distance degree");
		System.out.println("-overwrite - Overwrite output file without notice");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

//		 args = new String[10];
//		 args[0] = "/home/louai/nyc-taxi/yellowIndex";
//		 args[1] = "/home/louai/nyc-taxi/humanIndex";
//		 args[2] = "/home/louai/nyc-taxi/resultSTJoin";
//		 args[3] = "shape:edu.umn.cs.sthadoop.core.STPoint";
//		 args[4] =
//		 "rect:-74.98451232910156,35.04014587402344,-73.97936248779295,41.49399566650391";
//		 args[5] = "interval:2015-01-01,2015-01-02";
//		 args[6] = "timeDistance:1.day";
//		 args[7] = "spaceDistance:2";
//		 args[8] = "-overwrite";
//		 args[9] = "-no-local";

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
		
		if (params.get("timedistance") == null) {
			System.err.println("time distance is missing");
			printUsage();
			System.exit(1);
		}
		
		if (params.get("spacedistance") == null) {
			System.err.println("space distance is missing");
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
	    	inputstjoin = new Path(outputPath.getParent().toString() + "candidatebuckets");
	    }else{
	    	inputstjoin = new Path(outputPath.getParent().toString() + "/candidatebuckets");
	    }
	    
		long t1 = System.currentTimeMillis();
		long resultSize = stJoin(inputstjoin, outputPath, params);
		long t2 = System.currentTimeMillis();
		System.out.println("Total time: " + (t2 - t1) + " millis");
		System.out.println("Result size: " + resultSize);
	}

}
