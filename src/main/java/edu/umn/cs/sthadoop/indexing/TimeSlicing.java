package edu.umn.cs.sthadoop.indexing;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.sthadoop.core.STPoint;

public class TimeSlicing {
	public static SimpleDateFormat sdf;
	public static Shape inputShape;

	static enum TimeFormat {
		year, month, week, day, minute;
	}

	// mapper
	static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, NullWritable, Text> {
//		static SimpleDateFormat sdf;
		
		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			sdf = new SimpleDateFormat(job.get("time"));
			Class<?> classShape;
			try {
				classShape = Class.forName(job.get("shape"));
				inputShape = (Shape) classShape.newInstance();
			} catch (ClassNotFoundException e) {
			} catch (InstantiationException e) {
			} catch (IllegalAccessException e) {
			}
			
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {

				if (value.toString().contains(",")) {
					output.collect(NullWritable.get(), value);
				}
		
		}
	}

	 //Reducer
	static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				output.collect(new Text(NullWritable.get().toString()), values.next());
			}
		}
	}

	// Multiple output formats
	static class KeyBasedMultiFileOutput extends
			MultipleTextOutputFormat<NullWritable, Text> {
		
		
		@Override
		protected String generateFileNameForKeyValue(NullWritable key, Text value,
				String fileName) {
			String keyDate = "unkown";
			try {
//				if (value.toString().contains(",")) {
//					Date date;
//					String temp = value.toString().substring(0,
//							(value.toString().indexOf(",")));
//					date = sdf.parse(temp);
//					keyDate = sdf.toPattern() + "/" + sdf.format(date);
//					keyDate = keyDate.replace(":", "-");
//					keyDate = keyDate.replace(" ", "-");
//				}
				if (value.toString().contains(",")) {
				Date date;
				inputShape.fromText(value);
				STPoint obj = (STPoint) inputShape;
				String temp = obj.time;
				date = sdf.parse(temp);
				keyDate = sdf.toPattern() + "/" + sdf.format(date);
				keyDate = keyDate.replace(":", "-");
				keyDate = keyDate.replace(" ", "-");
			}
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
			}
			return keyDate + "/" + fileName;

		}
	}



	/**
	 * This method called to slice time based on time without considering the shape of the data
	 * @param inputPath
	 * @param outputPath
	 * @param spaceFormat
	 * @return
	 * @throws Exception
	 */
	public static int TemporalSliceMapReduce(Path inputPath, Path outputPath, String spaceFormat) throws Exception {
		switch (TimeFormat.valueOf(spaceFormat)) {
		case minute:
			spaceFormat = "yyyy-MM-dd HH:mm";
			break;
		case day:
			spaceFormat = "yyyy-MM-dd";
			break;
		case week:
			spaceFormat = "yyyy-MM-W";
			break;
		case month:
			spaceFormat = "yyyy-MM";
			break;
		case year:
			spaceFormat = "yyyy";
			break;
		default:
			spaceFormat = "yyyy-MM-dd";
			break;
		}
		sdf = new SimpleDateFormat(spaceFormat);
		
		JobConf conf = new JobConf(new Configuration(), TimeSlicing.class);
		FileSystem outfs = outputPath.getFileSystem(conf);
		outfs.delete(outputPath, true);
		conf.setJobName("Temporal Space Slicing");
		conf.set("time", spaceFormat);
		conf.setOutputKeyClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		// Mapper settings
		conf.setMapperClass(TimeSlicing.Map.class);
		ClusterStatus clusterStatus = new JobClient(conf).getClusterStatus();
		conf.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));
		// Reducer Settings
		conf.setNumReduceTasks(0);
		conf.setReducerClass(TimeSlicing.Reduce.class);
		conf.setCombinerClass(TimeSlicing.Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TimeSlicing.KeyBasedMultiFileOutput.class);

		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);
		JobClient.runJob(conf).waitForCompletion();
		return 0;
	}
	
	
	/**
	 * Consider the shape of the data when slicing the data. 
	 * @param inputPath
	 * @param outputPath
	 * @param params
	 * @return
	 * @throws Exception
	 */
	public static int TemporalSliceMapReduce(Path inputPath, Path outputPath, OperationsParams params) throws Exception {
		String spaceFormat = params.get("time");
		switch (TimeFormat.valueOf(spaceFormat)) {
		case minute:
			spaceFormat = "yyyy-MM-dd HH:mm";
			break;
		case day:
			spaceFormat = "yyyy-MM-dd";
			break;
		case week:
			spaceFormat = "yyyy-MM-W";
			break;
		case month:
			spaceFormat = "yyyy-MM";
			break;
		case year:
			spaceFormat = "yyyy";
			break;
		default:
			spaceFormat = "yyyy-MM-dd";
			break;
		}
		sdf = new SimpleDateFormat(spaceFormat);
		
		JobConf conf = new JobConf(new Configuration(), TimeSlicing.class);
//		TextSerializable inObj = params.getShape("shape");//OperationsParams.getTextSerializable(conf, "shape", null);
		TextSerializable inObj = params.getShape("shape");//OperationsParams.getTextSerializable(conf, "shape", null);
		if(inObj instanceof STPoint )
		{
//			inputShape =  inObj;
			FileSystem outfs = outputPath.getFileSystem(conf);
			outfs.delete(outputPath, true);
			conf.setJobName("Temporal Space Slicing");
			conf.set("time", spaceFormat);
			conf.set("shape", inObj.getClass().getName());
			conf.setOutputKeyClass(Text.class);
			conf.setMapOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			// Mapper settings
			conf.setMapperClass(TimeSlicing.Map.class);
			ClusterStatus clusterStatus = new JobClient(conf).getClusterStatus();
			conf.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));
			// Reducer Settings
			conf.setNumReduceTasks(0);
			conf.setReducerClass(TimeSlicing.Reduce.class);
			conf.setCombinerClass(TimeSlicing.Reduce.class);

			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TimeSlicing.KeyBasedMultiFileOutput.class);

			FileInputFormat.setInputPaths(conf, inputPath);
			FileOutputFormat.setOutputPath(conf, outputPath);
			JobClient.runJob(conf).waitForCompletion();
			
		}else{
			System.out.println("Not Instance of STPoint did not recognize the shape");
		}
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		// check if arguments length is 3.
		
//		 args = new String[4];
//		 args[0] = "/home/louai/nyc-taxi/yellow/output.txt";
//		 args[1] = "/home/louai/nyc-taxi/result/" ;
//		 args[2] = "shape:edu.umn.cs.sthadoop.core.STPoint";
//				 //"shape:edu.umn.cs.sthadoop.core.STpointsTweets";
//		 args[3] = "time:month";
			OperationsParams params = new OperationsParams(
					new GenericOptionsParser(args));
			Path inputPath = params.getInputPath();
			Path outputPath = params.getOutputPath();
			if (params.get("time") == null) {
				printUsage();
			}
			//TemporalSliceMapReduce(inputPath,outputPath,params.get("time"));

			TemporalSliceMapReduce(inputPath,outputPath, params);

	}

	private static void printUsage() {
		System.out.println("TimeBased Slicing");
		System.out.println("Parameters (* marks required parameters):");
		System.out.println("<input file> - (*) Path to input file");
		System.out.println("<output file> - (*) Path to output file");
		System.out.println("shape:<s> - Type of shapes stored in the file");
		System.out
				.println("time:<format> - slicing based on ISO Time format[ yyyy ,  yyyy-MM , yyyy-MM-W , yyyy-MM-dd , yyyy-MM-dd hh , yyyy-MM-dd hh:mm , yyyy-MM-dd hh:mm:ss ]");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}
}
