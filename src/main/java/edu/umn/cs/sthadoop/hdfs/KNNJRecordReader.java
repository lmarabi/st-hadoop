package edu.umn.cs.sthadoop.hdfs;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.mapreduce.RTreeRecordReader3;
import edu.umn.cs.spatialHadoop.mapreduce.SpatialRecordReader3;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;
import edu.umn.cs.sthadoop.core.STPoint;
import edu.umn.cs.sthadoop.mapreduce.SpatioTemporalInputFormat;
import edu.umn.cs.sthadoop.mapreduce.SpatioTemporalRecordReader;

/**
 *  
 * @author louai Alarabi
 *
 */

public class KNNJRecordReader<S extends STPoint> extends RecordReader<Partition, KNNJData<S>> {

	/** Logger for KNNJRecordReader */
	private static final Log LOG = LogFactory.getLog(KNNJRecordReader.class);

	private List<RecordReader<Partition, Iterable<S>>> internalReaders;
	public static OperationsParams params = null;
	KNNJData<S> knnjData;
	private boolean processed = false;

	@Override
	public void close() throws IOException {
		for (RecordReader<Partition, Iterable<S>> recordReader : internalReaders) {
			if (recordReader != null) {
				recordReader.close();
			}
		}
	}

	@Override
	public Partition getCurrentKey() throws IOException, InterruptedException {
		return internalReaders.get(0).getCurrentKey().clone();
	}

	@Override
	public KNNJData<S> getCurrentValue() throws IOException, InterruptedException {
		return knnjData;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 1 : 0;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		/* 
		 * In local mode, context is null. This code makes sure params is not null
		 * before passing it to getRecordReader.
		*/
		Configuration conf = context != null ? context.getConfiguration() : null;
		OperationsParams params = conf != null ? new OperationsParams(conf) : KNNJRecordReader.params;
		
		//System.out.println("input paths: "+KNNJRecordReader.params.getInputPaths().length);
		
		CombineFileSplit csplit = (CombineFileSplit) split;
		LOG.info("Splits in the CompineSplit: "+csplit);
		int numPaths = csplit.getNumPaths();
		internalReaders = new Vector<RecordReader<Partition, Iterable<S>>>(numPaths);
		this.knnjData = new KNNJData<S>();
		for (int i = 0; i < numPaths; i++) {
			FileSplit fsplit = new FileSplit(csplit.getPath(i), csplit.getOffset(i), csplit.getLength(i),
					csplit.getLocations());
			/*System.out.println(
					"path: " + csplit.getPath(i) + " " + csplit.getOffset(i) + " " + csplit.getLength(i));*/
			if (fsplit != null) {
				internalReaders.add(getRecordReader(fsplit, params));
			}
		}
		LOG.info(numPaths+" partitions initialized");
	}

	/*
	 * TODO Read records one by one instead of loading the whole data first
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!this.processed) {
			LOG.info("Start loading data from partitions");
			// Get the querySet first
//			while (internalReaders.get(0).nextKeyValue()) {
//				Iterable<S> shapes = internalReaders.get(0).getCurrentValue();
//				for (S shape : shapes) {
//					this.knnjData.qSet.add((S) shape.clone());
//				}				
//			}
			// Get the remaining Shapes
			for (int i = 0; i < internalReaders.size(); i++) {		
				while (internalReaders.get(i).nextKeyValue()) {
					Iterable<S> shapes = internalReaders.get(i).getCurrentValue();
					for (S shape : shapes) {
						this.knnjData.refSet.add((S) shape.clone());
					}
				}		
			}
			this.processed = true;
			LOG.info("Loading data is done");
			return true;
		}
		return false;
	}

	private RecordReader<Partition, Iterable<S>> getRecordReader(InputSplit split,
			OperationsParams params) throws IOException, InterruptedException {
		SpatioTemporalInputFormat<Partition, S> inputFormat = new SpatioTemporalInputFormat<Partition, S>();
		RecordReader<Partition, Iterable<S>> reader = inputFormat.createRecordReader(split, null);
		if (reader instanceof SpatioTemporalRecordReader) {
			((SpatioTemporalRecordReader<S>) reader).initialize(split, params);
		} else if (reader instanceof SpatialRecordReader3) {
			((SpatialRecordReader3<S>) reader).initialize(split, params);
		} else if (reader instanceof RTreeRecordReader3) {
			((RTreeRecordReader3<S>) reader).initialize(split, params);
		} else if (reader instanceof HDFRecordReader) {
			((HDFRecordReader) reader).initialize(split, params);
		} else {
			throw new RuntimeException("Unknown record reader");
		}
		return reader;
	}
}
