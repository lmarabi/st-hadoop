package edu.umn.cs.sthadoop.hdfs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.sthadoop.core.STPoint;

/**
 *  
 * @author louai Alarabi
 *
 */

public class HdfsInputFormat<S extends STPoint>
		extends FileInputFormat<Partition, KNNJData<S>> {

	/** Logger for KNNJSpatialInputFormat */
	private static final Log LOG = LogFactory.getLog(HdfsInputFormat.class);
	/** Create splits from two input files */
	public static final String KNNJoin = "SpatialInputFormat.KNNJoin";

	// This is load everything in memory separating the query points from the reference points
	@Override
	public RecordReader<Partition, KNNJData<S>>
			createRecordReader(InputSplit split, TaskAttemptContext context)
					throws IOException, InterruptedException {
		KNNJRecordReader<S> reader = new KNNJRecordReader<S>();
		return (RecordReader<Partition, KNNJData<S>>) reader;
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}
	

	protected void listStatus(final FileSystem fs, Path dir, final List<FileStatus> result) throws IOException {
		FileStatus[] fileStatus = fs.listStatus(dir, new PathFilter() {			
			@Override
			public boolean accept(Path path) {
				//return !path.getName().endsWith("~");
				return SpatialSite.NonHiddenFileFilter.accept(path);
			}
		});
		
		for (FileStatus status : fileStatus) {
			if (status.isDirectory()) {
				// Recursively go in subdir
				listStatus(fs, status.getPath(), result);
			} else {
				// A file, just add it
				result.add(status);
			}
		}
	}

	@Override
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		Configuration jobConf = job.getConfiguration();
		Path[] inputPaths = getInputPaths(job);
		//FileStatus[] statuses = {};
		List<FileStatus> result = new Vector<FileStatus>();

		FileSystem[] arrFS = new FileSystem[inputPaths.length];
		for (int i_fs = 0; i_fs < inputPaths.length; i_fs++) {
			arrFS[i_fs] = inputPaths[i_fs].getFileSystem(jobConf);
		}
		for (int i = 0; i < arrFS.length; i++) {
			listStatus(arrFS[i], inputPaths[i], result);
		}
		//statuses = arrFS[0].listStatus(inputPaths, SpatialSite.NonHiddenFileFilter);
		//result.addAll(Arrays.asList(statuses));
		LOG.info("listStatus returned " + result.size() + " files");
		return result;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		//List<InputSplit> splits = super.getSplits(job);		
		Configuration conf = job.getConfiguration();

		// Get a list of all input files. There should be exactly two files.
		final Path[] inputPaths = getInputPaths(job);
		Vector<GlobalIndex<Partition>> gIndexes = new Vector<GlobalIndex<Partition>>(
				2);

		// Extract global indexes from input files
		for (int i_input = 0; i_input < inputPaths.length; i_input++) {
			FileSystem fs = inputPaths[i_input].getFileSystem(conf);
			gIndexes.add(SpatialSite.getGlobalIndex(fs, inputPaths[i_input]));
		}
		// Overlapping partitions are combined in one CombineSplit
		List<InputSplit> matchedSplits = new Vector<InputSplit>();
		long t1 = System.currentTimeMillis();
		if (gIndexes.get(0) != null || gIndexes.get(1) != null) {
			matchedSplits = findMatchedSplits_Overlap_NN(gIndexes.get(0), gIndexes.get(1), job);
		}
		LOG.info("getSplits returned " + matchedSplits.size() + " splits");
		long t2 = System.currentTimeMillis() - t1;
		LOG.info("[stat:time:0=" + t2 + "]");
		return matchedSplits;
	}

	public List<InputSplit> findMatchedSplits_Overlap(GlobalIndex<Partition> ind1,
			GlobalIndex<Partition> ind2, JobContext job) throws IOException {
		// Overlapping partitions are combined in one CombineSplit
		final List<InputSplit> matchedSplits = new Vector<InputSplit>();
		for (Iterator<Partition> iter = ind1.iterator(); iter.hasNext();) {
			Partition p1 = (Partition) iter.next();
			List<Partition> partitions = new Vector<Partition>();
			partitions.add(p1);
			Iterator<Partition> iter2 = ind2.iterator();
			while (iter2.hasNext()) {
				Partition p2 = iter2.next();
				if (p1.x1 <= p2.x2 && p1.x2 >= p2.x1 && p1.y1 <= p2.y2
						&& p1.y2 >= p2.y1) {
					partitions.add(p2);
				}
			}
			matchedSplits.add(combineFileSplits(job, partitions));
			LOG.info(partitions.size());
		}
		return matchedSplits;
	}

	public List<InputSplit> findMatchedSplits_Overlap_NN(GlobalIndex<Partition> ind1,
			GlobalIndex<Partition> ind2, JobContext job) throws IOException {
		// Overlapping partitions are combined in one CombineSplit
		final List<InputSplit> mSplits = new Vector<InputSplit>();
		
		for (Iterator<Partition> iter1 = ind1.iterator(); iter1.hasNext();) {
			Partition part1 = (Partition) iter1.next();
			List<Partition> candidates = new Vector<Partition>();
			candidates.add(part1);
			List<Partition> pCandidates = new Vector<Partition>();
			
			for (Iterator<Partition> iter2 = ind2.iterator(); iter2.hasNext();) {
				Partition part2 = (Partition) iter2.next();
				if (part1.x1 <= part2.x2 && part1.x2 >= part2.x1 && part1.y1 <= part2.y2
						&& part1.y2 >= part2.y1) {
					pCandidates.add(part2);
				}
			}
			
			Set<Partition> pCandidatesSet = new HashSet<Partition>();
			pCandidatesSet.addAll(pCandidates);
			for (Iterator<Partition> iterator = pCandidatesSet.iterator(); iterator.hasNext();) {
				Partition partition = (Partition) iterator.next();
				candidates.add(partition);
			}
			mSplits.add(combineFileSplits(job, candidates));
		}
		return mSplits;
	}
	
	/**
	 * Combines a number of file splits into one CombineFileSplit. If number of
	 * splits to be combined is one, it returns this split as is without creating
	 * a CombineFileSplit.
	 * 
	 * @param splits
	 * @param startIndex
	 * @param count
	 * @return
	 * @throws IOException
	 */
	public static InputSplit combineFileSplits(JobContext job,
			List<Partition> partitions) throws IOException {
		Path[] paths = new Path[partitions.size()];
		long[] lengths = new long[partitions.size()];
		Path[] inputPaths = getInputPaths(job);
		Path firstPath = inputPaths[0];
		Path secondPath = inputPaths[1];
		paths[0] = new Path(firstPath, partitions.get(0).filename);
		lengths[0] = partitions.get(0).size;
		for (int i = 1; i < partitions.size(); i++) {
			paths[i] = new Path(secondPath, partitions.get(i).filename);
			lengths[i] = partitions.get(i).size;
		}
		return new CombineFileSplit(paths, lengths);
	}
}
