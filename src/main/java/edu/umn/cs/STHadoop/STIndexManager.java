package edu.umn.cs.STHadoop;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.STHadoop.TimeFormatST.TimeFormatEnum;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.operations.Repartition;

/***
 * This index manager will check will play a role in spatio-temporal index
 * maintenance, such that it do the following tasks: 1) check if there are
 * indexes need to be created in the lower resolution layer. For example, a week
 * index could be created from several days.
 * 
 * @author louai Alarabi
 *
 */

public class STIndexManager {
	/** Logger */
	private static final Log LOG = LogFactory.getLog(STIndexManager.class);

	private TimeFormatST timeFormat;

	private Path datasetPath;
	private Path indexesPath;
	private FileSystem fileSystem;

	private Path indexesHomePath;
	private Path sliceHomePath;

	private HashMap<String, Boolean> existIndexes;

	public STIndexManager(Path datasetPath, Path indexesPath, String time)
			throws Exception {
		try {
			this.timeFormat = new TimeFormatST(TimeFormatEnum.valueOf(time));

			this.datasetPath = datasetPath;
			this.indexesPath = indexesPath;

			this.fileSystem = this.indexesPath
					.getFileSystem(new Configuration());
			// specify the index and temporal slice path.
			indexesHomePath = new Path(this.indexesPath.toString() + "/"
					+ this.timeFormat.getSimpleDateFormat());

			sliceHomePath = new Path(this.datasetPath.getParent().toString() + "/slice/"
					+ this.timeFormat.getSimpleDateFormat());

			// check if there is a temporal slice otherwise input dataset need
			// to be temporaly sliced.
			System.out.println("Slicing ..... start checking .... ");
			temporalTimeSlicing(datasetPath, sliceHomePath, time);
			
			System.out.println("Init index hierarchy ...... ");
			initializeIndexesHierarchy();

			existIndexes = new HashMap<String, Boolean>();

			loadExistIndexesDictionary();
		} catch (IOException e) {
			LOG.error("Failed to initialize TemporalIndexManager: "
					+ e.getMessage());
			e.printStackTrace();
		}
	}

	/***
	 * This method check if there the data is already sliced or not other wise
	 * it will slice it.
	 * 
	 * @throws Exception
	 */
	private void temporalTimeSlicing(Path inputdata, Path outputdata,
			String time) throws Exception {
		System.out.println("The input data directory: "+ inputdata.toString()+
				"The output directory: "+ outputdata.toString());
		// check daily folder
		if (!this.fileSystem.exists(outputdata)) {
			TimeSlicing.TemporalSliceMapReduce(inputdata, outputdata, time);
		}else{
			System.out.println("The temporal sliced data exist");
		}
		
	}

	/**
	 * Creates folder for indexes if it does not exist, otherwise ignore.
	 * 
	 * @throws IOException
	 */
	private void initializeIndexesHierarchy() throws IOException {
		// check daily folder
		if (!this.fileSystem.exists(indexesHomePath)) {
			this.fileSystem.mkdirs(indexesHomePath);
		}

	}

	/**
	 * Loads information about exist indexes on this level yearly
	 * 
	 * @throws IOException
	 * @throws ParseException
	 */
	private void loadExistIndexesDictionary() throws IOException,
			ParseException {
		// load exist indexes.
		FileStatus[] indexesFiles = fileSystem.listStatus(indexesHomePath);
		for (FileStatus index : indexesFiles) {
			if (index.isDirectory()) {
				existIndexes.put(index.getPath().getName(), true);
			}
		}

		// load missing indexes by adding flag false to the value of hash map.
		FileStatus[] sliceFiles = fileSystem.listStatus(sliceHomePath);
		for (FileStatus slice : sliceFiles) {
			if (slice.isDirectory()) {
				System.out.println("The SliceName Found: "
						+ slice.getPath().getName());
				if (!existIndexes.containsKey(slice.getPath().getName())) {
					existIndexes.put(slice.getPath().getName(), false);
				}
			}
		}

	}

	/**
	 * This method return a list of paths that need to be build, which they
	 * don't exist in the index directory.
	 * 
	 * @return
	 */
	private String[] getNeedToBuildIndexes() {
		List<String> needed = new ArrayList<String>();
		// iterate over the the hashmap then return the list of non-existed
		// paths.
		for (Map.Entry<String, Boolean> pair : existIndexes.entrySet()) {
			if (!pair.getValue()) {
				needed.add(pair.getKey());
			}
		}

		// return path.
		String[] result = new String[needed.size()];
		for (int i = 0; i < result.length; i++) {
			result[i] = needed.get(i);
		}
		return result;
	}

	/**
	 * This method physically create spatialPartition for each temporal slice.
	 * 
	 * @throws IOException
	 */
	private void repartitionResolution(OperationsParams params)
			throws IOException {
		String[] list = this.getNeedToBuildIndexes();
		for (String temp : list) {
			System.out.println("Need to build This index: " + temp);
			Path inFile = new Path(sliceHomePath + "/" + temp);
			Path outputPath = new Path(indexesHomePath + "/" + temp);
			try {
				Repartition.repartition(inFile, outputPath, params);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void printUsage() {
		System.out
				.println("Performs a temporal indexing for data stored in hadoop");
		System.out.println("Parameters: (* marks required parameters)");
		System.out.println("<dataset path> - (*) Path to input dataset");
		System.out.println("<index path> - (*) Path to index output");
		System.out.println("time:[hour,day,week,month,year] - (*) Time Format");
		System.out.println("-overwrite - Overwrite output file without notice");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws Exception {
		// Parse parameters
		OperationsParams params = new OperationsParams(
				new GenericOptionsParser(args));
		Path datasetPath = params.getInputPath();
		Path indexesPath = params.getOutputPath();
		if (params.get("time") == null) {
			printUsage();
		}

		String time = params.get("time");

		// This constructor will check for what is needed to be done in the
		// spatiotemporal index.
		STIndexManager temporalIndexManager = new STIndexManager(datasetPath,
				indexesPath, time);
		// This method physically create spatial partitions.
		temporalIndexManager.repartitionResolution(params);

	}

}
