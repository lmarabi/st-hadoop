/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.sthadoop.operations;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.PriorityQueue;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.mapred.TextOutputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialInputFormat3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.sthadoop.core.STPoint;
import edu.umn.cs.sthadoop.hdfs.HdfsDataPartitions;
import edu.umn.cs.sthadoop.hdfs.HdfsInputFormat;
import edu.umn.cs.sthadoop.hdfs.HdfsRecordReader;

/**
 * Performs Spatio-temporal Join query over two indexed spatio-temporal datasets.
 * 
 * @author louai Alarabi
 *
 */
public class STJoins {
	/** Logger for KNNJoin */
	// private static final Log LOG = LogFactory.getLog(KNNJoin.class);
	private static final Log LOG = LogFactory.getLog(STJoins.class);

	public static OperationsParams params = null;

	static void println(Object line) {
		System.out.println(line);
	}

	/**
	 * Stores a shape text along with its distance to the query point. Notice that
	 * it cannot be a ShapeWithDistance because we cannot easily deserialize it
	 * unless we know the right class of the shape.
	 * 
	 * @author Ahmed Eldawy
	 *
	 */
	public static class TextWithDistance implements Writable, Cloneable, TextSerializable, Comparable<TextWithDistance> {
		public double distance;
		public Text text = new Text();

		public TextWithDistance() {
		}

		public TextWithDistance(double distance) {
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeDouble(distance);
			text.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			distance = in.readDouble();
			text.readFields(in);
		}

		@Override
		public Text toText(Text t) {
			TextSerializerHelper.serializeDouble(distance, t, ',');
			t.append(text.getBytes(), 0, text.getLength());
			return t;
		}

		@Override
		public int hashCode() {
			return this.text.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			return this.text.equals(((TextWithDistance) obj).text);
		}

		@Override
		public void fromText(Text t) {
			distance = TextSerializerHelper.consumeDouble(t, ',');
			text.set(t);
		}

		@Override
		public String toString() {
			return distance + "," + text;
		}

		@Override
		protected TextWithDistance clone() {
			TextWithDistance c = new TextWithDistance();
			c.distance = this.distance;
			c.text.set(this.text);
			return c;
		}

		@Override
		public int compareTo(TextWithDistance o) {
			return this.distance < o.distance ? -1 : (this.distance > o.distance ? +1 : 0);
		}
	}

	/** Stores a shape along with its distance from the query point */
	static class ShapeWithDistance<S extends Shape> implements Comparable<ShapeWithDistance<S>> {
		public S shape;
		public double distance;

		public ShapeWithDistance() {
		}

		public ShapeWithDistance(S s, double d) {
			this.shape = s;
			this.distance = d;
		}

		@Override
		public int compareTo(ShapeWithDistance<S> o) {
			return Double.compare(this.distance, o.distance);
		}

		@Override
		public String toString() {
			return shape.toString() + " @" + distance;
		}

		public Text toText(Text t, String delimiter) {
			byte[] bytes = delimiter.getBytes();
			t.append(bytes, 0, bytes.length);
			TextSerializerHelper.serializeDouble(distance, t, ',');
			return shape.toText(t);
		}

		public Text toText(Text t, S shape) {
			TextSerializerHelper.serializeDouble(distance, t, ',');
			return shape.toText(t);
		}
	}

	/**
	 * Keeps KNN objects ordered by their distance descending
	 * 
	 * @author louai alarabi 
	 *
	 */
	public static class KNNObjects<E extends Comparable<E>, S extends Shape> extends PriorityQueue<E> {
		/** Capacity of the queue */
		private int capacity;

		public KNNObjects(int k) {
			this.capacity = k;
			super.initialize(k);
		}

		/**
		 * Keep elements sorted in descending order (Max heap)
		 */
		@Override
		protected boolean lessThan(Object a, Object b) {
			return ((E) a).compareTo((E) b) > 0;
		}
	}

	private static RecordReader<Rectangle, Iterable<Shape>> getRecordReader(InputSplit split, OperationsParams params)
			throws IOException, InterruptedException {
		SpatialInputFormat3<Rectangle, Shape> inputFormat = new SpatialInputFormat3<Rectangle, Shape>();
		RecordReader<Rectangle, Iterable<Shape>> reader = inputFormat.createRecordReader(split, null);
		if (reader instanceof SpatialRecordReader3) {
			((SpatialRecordReader3) reader).initialize(split, params);
		} else if (reader instanceof RTreeRecordReader3) {
			((RTreeRecordReader3) reader).initialize(split, params);
		} else if (reader instanceof HDFRecordReader) {
			((HDFRecordReader) reader).initialize(split, params);
		} else {
			throw new RuntimeException("Unknown record reader");
		}
		return reader;
	}



	public static class STJoinsMapper<S extends STPoint> extends Mapper<Partition, HdfsDataPartitions<S>, NullWritable, Text> {
		/** A temporary object to be used for output */
		// private final TextWithDistance outputValue = new TextWithDistance();

		/** User query */
		private int k;
		Configuration conf;
		FileSystem fs;
		Path inputPath2;
		GlobalIndex<Partition> globalIndex2;
		Vector<String> partPath;
		Vector<Partition> splitPartitions;
		MultipleOutputs<NullWritable, Text> multipleOuts;
		NullWritable dummy = NullWritable.get();
		CombineFileSplit csplit;

		/** Counters */
		enum Stats {
			qSplits, refSplits, numQRecs, numRefRecs, phase2Recs
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			conf = context.getConfiguration();
			multipleOuts = new MultipleOutputs<NullWritable, Text>(context);
			k = conf.getInt("k", 1);
			csplit = ((CombineFileSplit) context.getInputSplit());
			if (csplit.getNumPaths() > 1) { // check if there are reference partitions.
				inputPath2 = csplit.getPath(1).getParent();
				globalIndex2 = SpatialSite.getGlobalIndex(inputPath2.getFileSystem(conf), inputPath2);
				splitPartitions = new Vector<Partition>();
				for (int i = 1; i < csplit.getNumPaths(); i++) {
					for (Partition p : globalIndex2) {
						if (csplit.getPath(i).getName().compareTo(p.filename) == 0) {
							splitPartitions.addElement(p);
						}
					}
				}
				context.getCounter(Stats.refSplits).increment(csplit.getNumPaths() - 1);
			}
			context.getCounter(Stats.qSplits).increment(1);
		}

		@Override
		protected void map(Partition key, HdfsDataPartitions<S> input, final Context context)
				throws IOException, InterruptedException {
//			final List<S> qSet = input.qSet;
			final List<S> refSet = input.refSet;
			//System.out.println("key in mapper: "+key.filename);
			
//			for (S queryShape : refSet) {
				for (S refShape : refSet) {
					context.write(dummy, refShape.toText(new Text()));
				}
//			}

		}

		public void serializeText(Text t, Vector<Partition> partitions) throws IOException, InterruptedException {
			csplit.getPaths()[0].getName();
			byte[] bytes = ("&" + csplit.getPaths()[0].getName()).getBytes();
			// byte[] bytes = ("&" + partitions.get(0).filename).getBytes();
			t.append(bytes, 0, bytes.length);
			for (int i = 0; i < partitions.size(); i++) {
				bytes = ("#" + partitions.get(i).filename).getBytes();
				t.append(bytes, 0, bytes.length);
			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			multipleOuts.close();
		}
	}

	public static void serializeText(Text t, Vector<Partition> partitions) throws IOException, InterruptedException {
		byte[] bytes = ("&" + partitions.get(0).filename).getBytes();
		t.append(bytes, 0, bytes.length);
		for (int i = 1; i < partitions.size(); i++) {
			bytes = ("#" + partitions.get(i).filename).getBytes();
			t.append(bytes, 0, bytes.length);
		}
	}

	public static <S extends Shape> Vector<ShapeWithDistance<S>> orderResults(KNNObjects<ShapeWithDistance<S>, S> knn) {
		Vector<ShapeWithDistance<S>> resultsOrdered = new Vector<ShapeWithDistance<S>>();
		// double KthDistance = knn.top().distance;
		resultsOrdered.setSize(knn.size());
		while (knn.size() > 0) {
			ShapeWithDistance<S> nextAnswer = knn.pop();
			resultsOrdered.set(knn.size(), nextAnswer);
		}
		return resultsOrdered;
	}

	static <S extends Shape> void write(Text text, Path outputPath) throws IOException {
		if (outputPath != null) {
			PrintStream ps = new PrintStream(new FileOutputStream(outputPath.toString(), true));
			ps.print(text);
			ps.println();
			ps.close();
		}
	}

	static void JoinMapReduce(OperationsParams params)
			throws IOException, InterruptedException, ClassNotFoundException {
		final Path[] inputPaths = params.getInputPaths();
		Path outputPath = params.getOutputPath();
		//final int k = params.getInt("k", 1);
		HdfsRecordReader.params = params;
		//System.out.println(params.getInputPaths().length);
		
		

		long t1 = System.currentTimeMillis();
		// phase 1
		params.set("type", "phase1");
		Job job = Job.getInstance(params, "ST-Join Phase1");
		job.setJarByClass( STJoinsMapper.class);
		job.setInputFormatClass(HdfsInputFormat.class);
		HdfsInputFormat.setInputPaths(job, inputPaths[0], inputPaths[1]);
		job.setMapperClass( STJoinsMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(TextOutputFormat3.class);
		TextOutputFormat3.setOutputPath(job, outputPath);
		MultipleOutputs.addNamedOutput(job, "phase2", TextOutputFormat3.class, Text.class, Text.class);

		// Submit the job
		if (job.waitForCompletion(true)) {
			LOG.info("[stat:job[0]");
		} else {
			LOG.info("[stat:job[1]");
			return;
		}
		long t2 = System.currentTimeMillis() - t1;
		t1 = System.currentTimeMillis();
		Counters counters = job.getCounters();
		long refSplits = counters.findCounter( STJoinsMapper.Stats.refSplits).getValue();
		long qSplits = counters.findCounter( STJoinsMapper.Stats.qSplits).getValue();
		long numRefRecs = counters.findCounter( STJoinsMapper.Stats.numRefRecs).getValue();
		long numQRecs = counters.findCounter( STJoinsMapper.Stats.numQRecs).getValue();
		long numP2Recs = counters.findCounter( STJoinsMapper.Stats.phase2Recs).getValue();
		String str = String.format(
				"stat:counters[refSplits=%s;qSplits=%s;numRefRecs=%s;" + "numQRecs=%s;numP2Recs=%s;t1=%s]", refSplits, qSplits,
				numRefRecs, numQRecs, numP2Recs, t2);
		LOG.info(str);
		// LOG.info("[stat:counter:refSplits="+refSplits+"]");
		// LOG.info("[stat:counter:qSplits="+qSplits+"]");
		// LOG.info("[stat:counter:numRefRecs="+numRefRecs+"]");
		// LOG.info("[stat:counter:numQRecs="+numQRecs+"]");
		// LOG.info("[stat:counter:numP2Recs="+numP2Recs+"]");
		/*
		 * for (Iterator<String> iterator = counters.getGroupNames().iterator();
		 * iterator.hasNext();) {
		 * String str = (String) iterator.next();
		 * LOG.info("[stat:counter="+str+"]");
		 * }
		 */
		// end of phase 1

		// phase 2
		/*params.set("type", "phase2");
		Job job2 = Job.getInstance(params, "KNNJoin Phase2");
		job2.setJarByClass(KNNJoin.class);
		job2.setMapperClass(TokenizerMapper.class);
		job2.setReducerClass(GroupingReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileSystem outputFS = outputPath.getFileSystem(params);
		Path p2OutPath;
		do {
			p2OutPath = new Path(outputPath.getParent(), outputPath.getName() + ".knnj_" + (int) (Math.random() * 1000000));
		} while (outputFS.exists(p2OutPath));
		FileSystem p2OutPathFS = FileSystem.get(p2OutPath.toUri(), params);

		job2.setInputFormatClass(KNNJInputFormatPhase2.class);
		KNNJInputFormatPhase2.setInputPaths(job2, outputPath);
		job2.setOutputFormatClass(TextOutputFormat3.class);
		TextOutputFormat3.setOutputPath(job2, p2OutPath);
		MultipleOutputs.addNamedOutput(job2, "phase3", TextOutputFormat3.class, NullWritable.class, Text.class);

		// Submit the job
		
		 * if (job2.waitForCompletion(true)) {
		 * LOG.info("Job2 succeeded.");
		 * } else {
		 * LOG.info("Job2 failed.");
		 * return;
		 * }
		 
		// end of phase 2

		t2 = System.currentTimeMillis() - t1;
		LOG.info("[stat:time:2=" + t2 + "]");
		t1 = System.currentTimeMillis();

		// phase 3
		params.set("type", "phase3");
		Job job3 = Job.getInstance(params, "KNNJoin Phase3");
		job3.setJarByClass(KNNJoin.class);

		job3.setMapperClass( STJoinsMapperPhase3.class);
		job3.setOutputKeyClass(NullWritable.class);
		job3.setOutputValueClass(Text.class);
		job3.setNumReduceTasks(0);

		Path p3OutPath;
		do {
			p3OutPath = new Path(outputPath.getParent(), outputPath.getName() + ".knnj_" + (int) (Math.random() * 1000000));
		} while (outputFS.exists(p3OutPath));
		FileSystem p3OutPathFS = FileSystem.get(p3OutPath.toUri(), params);

		job3.setInputFormatClass(KNNJInputFormatPhase3.class);
		KNNJInputFormatPhase3.setInputPaths(job3, p2OutPath, inputPaths[1]);
		job3.setOutputFormatClass(TextOutputFormat3.class);
		TextOutputFormat3.setOutputPath(job3, p3OutPath);

		// Submit the job
		
		 * if (job3.waitForCompletion(true)) {
		 * LOG.info("Job3 succeeded.");
		 * } else {
		 * LOG.info("Job3 failed.");
		 * return;
		 * }
		 
		// end of phase 3

		// cleaning temporary dirs and files
		p2OutPathFS.delete(p2OutPath, true);
		p3OutPathFS.delete(p3OutPath, true);

		t2 = System.currentTimeMillis() - t1;
		LOG.info("[stat:time:3=" + t2 + "]");*/
	}

	private static boolean isInputIndexed(OperationsParams params, Path[] inputPaths) throws IOException {
		boolean isIndexed = true;
		for (int i = 0; i < inputPaths.length; i++) {
			FileSystem fs = inputPaths[i].getFileSystem(params);
			isIndexed = isIndexed && isPathIndexed(inputPaths[i], fs);
		}
		return isIndexed;
	}

	private static boolean isPathIndexed(Path path, FileSystem fs) {
		// Getting global index if any
		final GlobalIndex<Partition> gIndex = SpatialSite.getGlobalIndex(fs, path);
		if (gIndex != null) {
			return true;
		}
		return false;
	}

	private static void printUsage() {
		System.out.println("Performs a kNN join operation between two indexed data,"
				+ "the query set file/directory (first input) and the reference set " + "file/directory (second input)");
		System.out.println("Parameters: (* marks required parameters)");
		System.out.println("<input file> - (*) Path to query input file");
		System.out.println("<input file> - (*) Path to reference input file");
		System.out.println("<output directory> - Path to output directory");
		System.out.println("k:<k> - (*) Number of neighbors to each point");
		System.out.println("shape:<shape> - shape on input data");
		System.out.println("-overwrite - Overwrite output file without notice");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws IOException, InterruptedException {
//		args = new String[5];
//		args[0] = "/home/louai/nyc-taxi/taxiIndex/yyyy-MM-dd/2015-01-01"; 
//		args[1] = "/home/louai/nyc-taxi/taxiIndex/yyyy-MM-dd/2015-01-02";
//		args[2] = "/home/louai/nyc-taxi/outputRami";
//		args[3] = "shape:edu.umn.cs.sthadoop.core.STPoint";
//		args[4] = "-overwrite";
		
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));

		/*
		 * String property = params.get("namenodes");
		 * System.out.println(property);
		 */
		
		
		
		Path[] paths = params.getPaths();
		if (paths.length <= 2 && !params.checkInput()) {
			printUsage();
			System.exit(1);
		}
		final Path[] inputPaths = params.getInputPaths();
		LOG.info("Number of input paths: " + inputPaths.length);

		final Path userOutputPath = paths.length > 2 ? paths[2] : null;
		if (userOutputPath != null) {
			String newOutputPathStr = userOutputPath.toString() + "/" + inputPaths[0].getName() + "."
					+ inputPaths[1].getName();
			params.setOutputPath(newOutputPathStr);
			params.checkInputOutput();
		} else {
			printUsage();
			System.exit(1);
		}

		final int k = params.getInt("k", 1);
		if (k == 0) {
			LOG.warn("k = 0");
		}

		if (!isInputIndexed(params, inputPaths)) {
			System.out.println("There is no index file in one or both inputs");
			if (params.getBoolean("local", false)) {
//				localKNNJoin(inputPaths, params.getOutputPath(), params);
			} else {
				System.exit(1);
			}
		} else {
			long t1 = System.currentTimeMillis();
			try {
				JoinMapReduce(params);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			long t2 = System.currentTimeMillis();
			LOG.info("[stat:time:overall=" + (t2 - t1) + "]");
		}
	}
}
