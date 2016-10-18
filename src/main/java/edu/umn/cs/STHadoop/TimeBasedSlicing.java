package edu.umn.cs.STHadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
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
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.STHadoop.STSampler.Map.Conversion;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.STPoint;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.Text2;
import edu.umn.cs.spatialHadoop.io.TextSerializable;

public class TimeBasedSlicing{
	
	 private static final Log LOG = LogFactory.getLog(TimeBasedSlicing.class);

	// Define the time based slicing parameter
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	// Determine the shape of the text input file
	/**Shape instance used to parse input lines*/
	static Shape inShape;


	// mapper
	static class Map extends MapReduceBase implements
			Mapper<Object, Text, Text, Text> {

		private String spaceFormat;

		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			super.configure(job);
			spaceFormat = job.get("time");
			sdf = new SimpleDateFormat(spaceFormat);
			inShape = OperationsParams.getShape(job, "shape");
			
		}

		@Override
		public void map(Object key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
//			String temp = obj.t;
			try {
				inShape.fromText(value);
				Date date;
//				String temp = value.toString().substring(0,
//						(value.toString().indexOf(",")));
				date = sdf.parse(inShape.t);
				output.collect(new Text(Integer.toString(date.getMonth())), value);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
			} catch ( IndexOutOfBoundsException e){
			}
				
		}
	}

	// Reducer
	static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
//			System.out.println("Reducer --> Value of reducer: "+ key);
			while (values.hasNext()) {
				output.collect(new Text((String)null), values.next());
			}
		}
	}

	// Multiple output formats
	static class KeyBasedMultiFileOutput extends
			MultipleTextOutputFormat<Text, Text> {
		
		@Override
		protected String generateFileNameForKeyValue(Text key, Text value,
				String fileName) {
			String keyDate = "unkown";
			try {
//				LOG.info("Value of reducer: "+ value);
//				System.out.println("multioutput --> Value of reducer: "+ value);
//				inshape.fromText(value);
//				if (inshape instanceof STPoint) {
//					STPoint obj = (STPoint) inshape;
					Date date;
					String temp = value.toString().substring(0,
							(value.toString().indexOf(",")));
//					String temp = obj.t;
					date = sdf.parse(temp);
					keyDate = sdf.format(date);
					keyDate = keyDate.replace(":", "-");
					keyDate = keyDate.replace(" ", "-");
					
//				}
				return sdf.toPattern() + "/" + keyDate + "/" + fileName;
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
				return sdf.toPattern() + "/" + keyDate + "/" + fileName;
			} catch ( IndexOutOfBoundsException e){
				return sdf.toPattern() + "/" + keyDate + "/" + fileName;
			}

		}
	}
	
	 private static void printUsage() {
//			if (args.length != 3) {
//			System.out
//					.println("=== Temporal Slicing Program === \n"
//							+ "Takes 3 parameters separated by space:\n"
//							+ "input file\n"
//							+ "output Dirctory\n"
//							+ "ISO Time format [ yyyy ,  yyyy-MM , yyyy-MM-W , yyyy-MM-dd , yyyy-MM-dd hh , yyyy-MM-dd hh:mm , yyyy-MM-dd hh:mm:ss ]\n"
//							+ "Example: /input.txt /outDir YYYY-MM");
//			return 0;
//		}
		
		/*
		 * yyyy-MM-dd-hh.mm.ss Possible temporal space partitions. - Seconds:
		 * yyyy-MM-dd HH:mm:ss - Minutes: yyyy-MM-dd HH:mm - Hours: yyyy-MM-dd
		 * HH - Day: yyyy-MM-dd - Week yyyy-MM-W not directly supported by the
		 * simpleDateFormat. - Month: yyyy-MM Year: yyyy
		 */
		    System.out.println("TimeBased Slicing");
		    System.out.println("Parameters (* marks required parameters):");
		    System.out.println("<input file> - (*) Path to input file");
		    System.out.println("<output file> - (*) Path to output file");
		    System.out.println("shape:<s> - Type of shapes stored in the file");
		    System.out.println("time:<format> - slicing based on ISO Time format[ yyyy ,  yyyy-MM , yyyy-MM-W , yyyy-MM-dd , yyyy-MM-dd hh , yyyy-MM-dd hh:mm , yyyy-MM-dd hh:mm:ss ]");
		    GenericOptionsParser.printGenericCommandUsage(System.out);
		  }
	 
	 private static int slicingMapReduce(Path inPath, Path outPath,
		      OperationsParams paramss) throws IOException, InterruptedException,
		      ClassNotFoundException {
		JobConf job = new JobConf(paramss, TimeBasedSlicing.class);
		FileSystem outfs = outPath.getFileSystem(job);
		outfs.delete(outPath, true);
		job.setJobName("TimeBasedSlicing");
		if (paramss.get("time") == null)
			paramss.set("time", "yyyy-MM");
		job.set("time", paramss.get("time"));
		//job.setOutputKeyClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(TimeBasedSlicing.Map.class);
		job.setReducerClass(TimeBasedSlicing.Reduce.class);
		job.setCombinerClass(TimeBasedSlicing.Reduce.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TimeBasedSlicing.KeyBasedMultiFileOutput.class);
		
		ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
	    job.setNumMapTasks(clusterStatus.getMaxMapTasks() * 5);
	    // Number of reduces can be set to zero. However, setting it to a reasonable
	    // number ensures that number of output files is limited to that number
	    job.setNumReduceTasks(
	        Math.max(1, clusterStatus.getMaxReduceTasks() * 9 / 10));
//		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);

		// RunningJob run_job = JobClient.runJob(job);
		try {
			JobClient.runJob(job).waitForCompletion();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// JobClient.runJob(job).waitForCompletion();
		return 0;

	 }
	 
	
	 
	 public static int timeBasedSlicing(Path inPath, Path outPath, OperationsParams params)
		      throws IOException, InterruptedException, ClassNotFoundException {
		      return slicingMapReduce(inPath, outPath, params);
		  }

	
	public static void main(String[] args) throws Exception {
//		args = new String[3];
//		args[0] = "/export/scratch/louai/scratch1/workspace/dataset/sthadoop/input";
//		args[1] = "/export/scratch/louai/scratch1/workspace/dataset/sthadoop/output";
//		args[2] = "shape:edu.umn.cs.STHadoop.TestSTPoint";

//		Path InputFiles = (args[0].length() > 0) ? new Path(args[0])
//				: new Path("/export/scratch/louai/scratch1/workspace/dataset/sthadoop/input");// args[0];
//		Path OutputDir = (args[1].length() > 0) ? new Path(args[1])
//				: new Path("/export/scratch/louai/scratch1/workspace/dataset/sthadoop/output");// args[1];
//		String value = (args[2].length() > 0) ? args[2] : "yyyy-MM";
		 OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		 Path inputPath = params.getInputPath();
		    Path outputPath = params.getOutputPath();
		  if (!params.checkInput()) {
		      printUsage();
		      System.exit(1);
		    }
		  long t1 = System.currentTimeMillis();
		  int exit = timeBasedSlicing(inputPath,outputPath,params);
		  long t2 = System.currentTimeMillis();
		   System.out.println("Total slicing time in millis "+(t2-t1));
		   System.exit(exit);

	}
}
