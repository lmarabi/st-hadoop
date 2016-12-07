package edu.umn.cs.STHadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
// mapper 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
// Reducer
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;

/**
 * Driver
 * @author louai
 *
 */

public class TimeSlicing {

	
	public static int timeslicing(Path input, Path output, String timeFormat) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("time", timeFormat);
		Job job = new Job(conf);
		FileSystem outfs = output.getFileSystem(conf);
		outfs.delete(output, true);
		job.setJobName("MultipleOutputs example");
		job.setJarByClass(TimeSlicing.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapperClass(MapperFormatMultiOutput.class);
		job.setMapOutputKeyClass(Text.class);
		job.setReducerClass(ReducerFormatMultiOutput.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
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

	public static void main(String[] args) throws Exception {
//		args = new String[3];
//		args[0] = "/export/scratch/louai/scratch1/workspace/dataset/idea-stHadoop/data/temporalSlice/input/2015-random/input1";
//		args[1] = "/export/scratch/louai/scratch1/workspace/dataset/idea-stHadoop/data/temporalSlice/output/"; 
//		args[2] = "time:yyyy-MM";
		OperationsParams params = new OperationsParams(
				new GenericOptionsParser(args));
		Path inputPath = params.getInputPath();
		Path outputPath = params.getOutputPath();
		String timeFormat = params.get("time");
		int exitCode = timeslicing(inputPath, outputPath, timeFormat); 
		System.exit(exitCode);
	}
}


/*

public class DriverFormatMultiOutput extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.out
					.printf("Two parameters are required for DriverFormatMultiOutput- <input dir> <output dir> <time-format>\n");
			return -1;
		}
		Path inputDir = new Path(args[0]);
		Path OutputDir = new Path(args[1]); 
		String timeFormat = args[2];
		
		Configuration conf = getConf(); 
		conf.set("time", timeFormat);
		Job job = new Job(conf);
		FileSystem outfs = OutputDir.getFileSystem(getConf());
		outfs.delete(OutputDir, true);
		job.setJobName("MultipleOutputs example");
		job.setJarByClass(DriverFormatMultiOutput.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, inputDir);
		FileOutputFormat.setOutputPath(job, OutputDir);
		
		job.setMapperClass(MapperFormatMultiOutput.class);
		job.setMapOutputKeyClass(Text.class);
		job.setReducerClass(ReducerFormatMultiOutput.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
//		args = new String[2];
//		args[0] = "/export/scratch/louai/scratch1/workspace/dataset/idea-stHadoop/data/temporalSlice/input/2015-random/input1";
//		args[1] = "/export/scratch/louai/scratch1/workspace/dataset/idea-stHadoop/data/temporalSlice/output/"; 
		int exitCode = ToolRunner.run(new Configuration(),
				new DriverFormatMultiOutput(), args);
		System.exit(exitCode);
	}
}

*/

/**
 * Mapper 
 * @author louai
 *
 */
class MapperFormatMultiOutput extends Mapper<LongWritable, Text, Text, Text> {

	private Text txtKey = new Text("");
	private SimpleDateFormat sdf ;//= new SimpleDateFormat("yyyy-MM-dd");

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String time = context.getConfiguration().get("time");
		sdf = new SimpleDateFormat(time);
		if (value.toString().contains(",")) {
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
			txtKey = new Text(sdf.toPattern()+"/"+keyDate+ "/data");
			context.write(txtKey, value);
		}

	}
}

/**
 * Reducer 
 * @author louai
 *
 */
class ReducerFormatMultiOutput extends Reducer<Text, Text, Text, Text> {

	private MultipleOutputs<Object, Text> mos;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs(context);

	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text value : values) {
			mos.write(NullWritable.get(), value, key.toString());

		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

}
