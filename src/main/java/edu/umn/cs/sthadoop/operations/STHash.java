package edu.umn.cs.sthadoop.operations;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.sthadoop.core.STPoint;

public class STHash {
	/** Class logger */
	private static final Log LOG = LogFactory.getLog(STHash.class);
	
	static class STHashMap extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		private double degree = 0.1;
		private double x1 = -180;
		private double y1 = -90;
	
		
		@Override
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			if(value != null){
			STPoint shape = new STPoint();
			shape.fromText(value);
			// Hasing objects to the grid. 
			int columnID = (int)((shape.x - x1)/ degree);
			int rowID = (int)((shape.y - y1)/ degree);
			output.collect(new LongWritable((columnID*rowID+1)), shape.toText(new Text()));
			}
		}
	}

	static class STHashReduce extends MapReduceBase implements 
	Reducer<LongWritable, Text, LongWritable, Text> {		
	
		@Override
		public void reduce(final LongWritable cellId, Iterator<Text> values, 
				final OutputCollector<LongWritable,Text> output,Reporter reporter) throws IOException {
//			LinkedList<STPoint> shapes = new LinkedList<STPoint>();
			StringBuilder txt = new StringBuilder();
			STPoint shape = new STPoint();
			
			Text temp = new Text(); 
			while(values.hasNext()){
				temp = values.next();
				txt.append("\t");
				txt.append(temp.toString());
			}
			output.collect(cellId, new Text(txt.toString()));
			
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
	public static long stHash(Path inputPath, Path outputPath, OperationsParams params)
			throws IOException, Exception, InterruptedException {
		
		JobConf conf = new JobConf(new Configuration(), STHash.class);
		FileSystem outfs = outputPath.getFileSystem(conf);
		outfs.delete(outputPath, true);
		conf.setJobName("STJoin Hashing");
		// pass params to the join map-reduce 
		conf.set("timedistance", params.get("timedistance"));
		conf.set("spacedistance", params.get("spacedistance"));
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);
		// Mapper settings
		conf.setMapperClass(STHashMap.class);
		conf.setReducerClass(STHashReduce.class);
		conf.setCombinerClass(STHashReduce.class);
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, inputPath);
		FileOutputFormat.setOutputPath(conf, outputPath);
		conf.setNumReduceTasks(30);
		JobClient.runJob(conf).waitForCompletion();
		outfs = inputPath.getFileSystem(conf);
		outfs.delete(inputPath);
		return 0;
	}

}
