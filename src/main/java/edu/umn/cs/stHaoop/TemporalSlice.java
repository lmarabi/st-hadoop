package edu.umn.cs.stHaoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

/**
 * @author  louai Alarabi
 *
 */
public class TemporalSlice {

	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	// mapper
	static class Map extends MapReduceBase implements
			Mapper<Object, Text, Text, Text> {
		
		private String spaceFormat; 
		
		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			super.configure(job);
			spaceFormat = job.get("TSpace");
			sdf = new SimpleDateFormat(spaceFormat);
		}
		
		@Override
		public void map(Object key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			output.collect(null, value);
		}
	}

	// Reducer
	static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				output.collect(null, values.next());
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
				
				Date date;
				String temp = value.toString().substring(0,
						(value.toString().indexOf(",")));
				date = sdf.parse(temp);
				keyDate = sdf.format(date);
				keyDate = keyDate.replace(":", "-");
				keyDate = keyDate.replace(" ", "-");
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch ( IndexOutOfBoundsException e){
				e.printStackTrace();
			}
			return sdf.toPattern()+"/"+keyDate + "/" + fileName;
		}
	}

	public static void main(String[] args) throws Exception {
		Path InputFiles = new Path(
				"/export/scratch/louai/scratch1/workspace/dataset/idea-stHadoop/data/temporalSlice/input.txt");// args[0];
		Path OutputDir = new Path(
				"/export/scratch/louai/scratch1/workspace/dataset/idea-stHadoop/data/temporalSlice/output/");// args[1];
		String value = "YYYY-MM-W";
		/*
		 * Possible temporal space partitions. 
		 * - Seconds: YYYY-MM-dd HH:mm:ss
		 * - Minutes: YYYY-MM-dd HH:mm
		 * - Hours: YYYY-MM-dd HH
		 * - Day: YYYY-MM-dd
		 * - Week YYYY-MM-W ++++ This you need to handle it internally as it is not directly supported by the simpleDateFormat. 
		 * - Month: YYYY-MM
		 * - Year: YYYY
		 * */
		

		JobConf conf = new JobConf(new Configuration(), TemporalSlice.class);
		FileSystem outfs = OutputDir.getFileSystem(conf);
		outfs.delete(OutputDir, true);
		conf.set("TSpace", value);
		conf.setOutputKeyClass(Text.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(TemporalSlice.Map.class);
		conf.setReducerClass(TemporalSlice.Reduce.class);
		conf.setCombinerClass(TemporalSlice.Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TemporalSlice.KeyBasedMultiFileOutput.class);
		conf.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(conf, InputFiles);
		FileOutputFormat.setOutputPath(conf, OutputDir);
		JobClient.runJob(conf).waitForCompletion();

	}
}
