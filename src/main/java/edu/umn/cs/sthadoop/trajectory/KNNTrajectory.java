package edu.umn.cs.sthadoop.trajectory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Circle;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.sthadoop.core.STNycSTPoints;
import edu.umn.cs.sthadoop.core.STNycTrajectory;
import edu.umn.cs.sthadoop.core.STPoint;
import edu.umn.cs.sthadoop.core.STRectangle;
import edu.umn.cs.sthadoop.operations.STRangeQuery;

/***
 * Performs k Nearest Neighbor (kNN) query for trajectories Support Dynamic time
 * Warping Similarity, Edit Distance Similarity, and Frechet Similarity
 * 
 * @author Louai Alarabi
 *
 */
public class KNNTrajectory {
	static final Log LOG = LogFactory.getLog(KNNTrajectory.class);

	/**
	 * Compute the MBR of a given trajectory.
	 * 
	 * @param trajectory
	 * @return rectangle
	 */
	private static String getTrajectoryRectangle(String trajectory) {
		String[] trajectoryPoints = trajectory.split("x");
		double x1 = Double.parseDouble(trajectoryPoints[0].split(",")[0]);
		double y1 = Double.parseDouble(trajectoryPoints[0].split(",")[1]);
		double x2 = x1;
		double y2 = y1;
		double tempDouble;
		for (String p : trajectoryPoints) {
			String[] point = p.split(",");
			// get the minMax of x point
			tempDouble = Double.parseDouble(point[0]);
			if (tempDouble <= x1)
				x1 = tempDouble;
			if (tempDouble >= x2)
				x2 = tempDouble;
			// get the minMax of y.
			tempDouble = Double.parseDouble(point[1]);
			if (tempDouble <= y1)
				y1 = tempDouble;
			if (tempDouble >= y2)
				y2 = tempDouble;
		}
		return Double.toString(x1) + "," + Double.toString(y1) + ","
				+ Double.toString(x2) + "," + Double.toString(y2);

	}

	private static class CirclePoint {
		public Point p;
		public double distance;

		public CirclePoint(Point p, double dist) {
			this.p = p;
			this.distance = dist;
		}

	}

	/**
	 * This function draw the test circle around the candidate list of k points.
	 * 
	 * @param trajectory
	 * @param candidates
	 * @param queue
	 * @return MBR
	 */
	private static CirclePoint getMaximumDistance(String trajectory,
			HashMap<String, List<STPoint>> candidates,
			PriorityQueue<KNNTrajectory.Score> queue) {
		String minMax = getTrajectoryRectangle(trajectory);
		double x1 = Double.parseDouble(minMax.split(",")[0]);
		double y1 = Double.parseDouble(minMax.split(",")[1]);
		Point min = new Point(x1, y1);
		double x2 = Double.parseDouble(minMax.split(",")[2]);
		double y2 = Double.parseDouble(minMax.split(",")[3]);
		Point max = new Point(x2, y2);
		double maximumDistance = min.distanceTo(max);
		CirclePoint circlePoint = new CirclePoint(min, maximumDistance);
		Score[] kthpoints = new Score[queue.size()];
		queue.toArray(kthpoints);
		double tempdist = 0f;
		for (Score k : kthpoints) {
			List<STPoint> points = candidates.get(k.id);
			for (STPoint p : points) {
				if (maximumDistance < p.distanceTo(min)) {
					maximumDistance = p.distanceTo(min);
					circlePoint = new CirclePoint(new Point(min.x, min.y),
							maximumDistance);

				}
				if (maximumDistance < p.distanceTo(max)) {
					maximumDistance = p.distanceTo(max);
					circlePoint = new CirclePoint(new Point(max.x, max.y),
							maximumDistance);
				}
			}
		}

		return circlePoint;
	}

	/**
	 * return the query trajectory as a list.
	 * 
	 * @param trajectory
	 * @return trajectory points as a list.
	 */
	private static List<STPoint> getTrajectoryQueryPoints(String trajectory) {
		List<STPoint> list = new ArrayList<STPoint>();
		String[] trajectoryPoints = trajectory.split("x");
		for (int i = 0; i < trajectoryPoints.length; i++) {
			STPoint temp = new STPoint();
			temp.fromText(new Text("0000-00-00,"
					+ trajectoryPoints[i].split(",")[0] + ","
					+ trajectoryPoints[i].split(",")[1]));
			list.add(temp);
		}
		return list;
	}

	private static void printUsage() {
		System.out.println("Performs a KNN DTW query on an input file");
		System.out.println("Parameters: (*) marks required parameters)");
		System.out.println("<input file> - (*) Path to input file");
		System.out.println("<output file> -  Path to input file");
		System.out
				.println("shape:<shape:edu.umn.cs.sthadoop.trajectory.GeolifeTrajectory> - (*) Type of shapes stored in input file");
		System.out
				.println("interval:<date1,date2> - (*) Temporal query range. "
						+ "Format of each date is yyyy-mm-dd");
		System.out.println("time:[day,week,month,year] -  (*) Time Format");
		System.out.println("k:<k> - (*) Number of neighbors to file");
		System.out
				.println("traj:<x1,y1;....;xn,yn>  - (*) the Full trajectory");
		System.out.println("-overwrite - Overwrite output file without notice");
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}

	public static void main(String[] args) throws Exception {

		// with specifying the level
		// args = new String[9];
		// args[0] = "/export/scratch/mntgData/summit/dataset/str";
		// args[1] =
		// "/export/scratch/mntgData/summit/dataset/result/knn-dis-result";
		// args[2] = "shape:edu.umn.cs.sthadoop.core.STNycSTPoints";
		// args[3] = "interval:2014-10-01,2014-10-01";
		// args[4] = "time:day";
		// args[5] = "k:100";
		// args[6] =
		// "traj:-73.9972915649414,40.682044982910156x-73.997872,40.682957x-73.997551,40.683629x-73.99722599999998,40.684298x-73.99635850000001,40.6860951x-73.99604099999999,40.686752999999996x-73.995732,40.687397000000004x-73.995497,40.687884999999994x-73.995333,40.688226x-73.995001,40.688915x-73.994678,40.689583x-73.9943596,40.69024279999999x-73.994046,40.69089700000001x-73.99373600000001,40.691534999999995x-73.993438,40.692153999999995x-73.99310299999999,40.692846x-73.992744,40.693605x-73.9924155,40.6942755x-73.99210000000001,40.694947x-73.99156690000001,40.6961229x-73.9914998,40.6961646x-73.9913637,40.696187900000005x-73.99116620000001,40.6961937x-73.99004120000001,40.696146600000006x-73.98896040000002,40.69610659999999x-73.98885870000001,40.6961025x-73.9886937,40.69609799999999x-73.98868159999998,40.696299499999995x-73.9886783,40.6963508x-73.98853940000001,40.6985153x-73.9884958,40.699194999999996x-73.98850279999999,40.6993448x-73.98853709999999,40.69946839999999x-73.9885972,40.6995985x-73.98867440000001,40.69972220000001x-73.9887689,40.699839299999994x-73.9888804,40.6999434x-73.9890264,40.7000605x-73.98934849999999,40.70025920000001x-73.9895549,40.7003964x-73.9897835,40.7005765x-73.990101,40.700835x-73.9903534,40.701042799999996x-74.0018019,40.7101218x-74.00217540000001,40.71041989999999x-74.0024481,40.7106376x-74.0036669,40.711607x-74.00414320000002,40.71196679999999x-74.0043619,40.712142899999996x-74.00442319999999,40.712206200000004x-74.0044588,40.712255299999995x-74.0044942,40.71232450000001x-74.0045008,40.712368600000005x-74.00450859999998,40.7124146x-74.00450409999999,40.712497299999995x-74.0044765,40.71260590000001x-74.0044099,40.7127226x-74.00437769999999,40.71282119999999x-74.0046331,40.7126318x-74.00485059999998,40.7124788x-74.0049167,40.7124347x-74.0052547,40.7122771x-74.0056323,40.7121466x-74.0057898,40.71209029999999x-74.0059794,40.712036700000006x-74.00621570000001,40.711960499999996x-74.00692569999998,40.7117603x-74.0070243,40.711735600000004x-74.00711779999999,40.7117118x-74.00733190000001,40.7117116x-74.0075469,40.71166989999999x-74.00767390000001,40.7116745x-74.0077635,40.7116988x-74.007823,40.7117257x-74.0078875,40.7117675x-74.00799730000001,40.7118889x-74.0081432,40.7119995x-74.00865820000001,40.7113879x-74.00845790000001,40.711247799999995x-74.0073597,40.710587700000005x-74.0073263,40.710550299999994x-74.00607600000001,40.70991779999999x-74.0060473,40.7099466";
		// args[7] = "-overwrite";
		// args[8] = "-no-local";// "-local";

		// without specifying the level
		// args = new String[8];
		// args[0] =
		// "/export/scratch/mntgData/geolifeGPS/geolife_Trajectories_1.3/HDFS/index_geolife";
		// args[1] =
		// "/export/scratch/mntgData/geolifeGPS/geolife_Trajectories_1.3/HDFS/knn-dis-result";
		// args[2] = "shape:edu.umn.cs.sthadoop.trajectory.GeolifeTrajectory";
		// args[3] = "interval:2008-05-01,2008-05-31";
		// args[4] = "k:100";
		// args[5] =
		// "traj:39.9119983,116.606835x39.9119783,116.6065483x39.9119599,116.6062649x39.9119416,116.6059899x39.9119233,116.6057282x39.9118999,116.6054783x39.9118849,116.6052366x39.9118666,116.6050099x39.91185,116.604775x39.9118299,116.604525x39.9118049,116.6042649x39.91177,116.6040166x39.9117516,116.6037583x39.9117349,116.6035066x39.9117199,116.6032666x39.9117083,116.6030232x39.9117,116.6027566x39.91128,116.5969383x39.9112583,116.5966766x39.9112383,116.5964232x39.9112149,116.5961699x39.9111933,116.5959249x39.9111716,116.5956883";
		// args[6] = "-overwrite";
		// args[7] = "-no-local";// "-local";

		// with segmentation based.
		// args = new String[9];
		// args[0] =
		// "/export/scratch/mntgData/summit/dataset/str+";
		// args[1] =
		// "/export/scratch/mntgDa	ta/summit/dataset/result/knn-dis-result";
		// args[2] = "shape:edu.umn.cs.sthadoop.core.STNycTrajectory";
		// args[3] = "interval:2014-10-01,2014-10-01";
		// args[4] = "time:day";
		// args[5] = "k:100";
		// args[6] =
		// "traj:-73.9972915649414,40.682044982910156x-73.997872,40.682957x-73.997551,40.683629x-73.99722599999998,40.684298x-73.99635850000001,40.6860951x-73.99604099999999,40.686752999999996x-73.995732,40.687397000000004x-73.995497,40.687884999999994x-73.995333,40.688226x-73.995001,40.688915x-73.994678,40.689583x-73.9943596,40.69024279999999x-73.994046,40.69089700000001x-73.99373600000001,40.691534999999995x-73.993438,40.692153999999995x-73.99310299999999,40.692846x-73.992744,40.693605x-73.9924155,40.6942755x-73.99210000000001,40.694947x-73.99156690000001,40.6961229x-73.9914998,40.6961646x-73.9913637,40.696187900000005x-73.99116620000001,40.6961937x-73.99004120000001,40.696146600000006x-73.98896040000002,40.69610659999999x-73.98885870000001,40.6961025x-73.9886937,40.69609799999999x-73.98868159999998,40.696299499999995x-73.9886783,40.6963508x-73.98853940000001,40.6985153x-73.9884958,40.699194999999996x-73.98850279999999,40.6993448x-73.98853709999999,40.69946839999999x-73.9885972,40.6995985x-73.98867440000001,40.69972220000001x-73.9887689,40.699839299999994x-73.9888804,40.6999434x-73.9890264,40.7000605x-73.98934849999999,40.70025920000001x-73.9895549,40.7003964x-73.9897835,40.7005765x-73.990101,40.700835x-73.9903534,40.701042799999996x-74.0018019,40.7101218x-74.00217540000001,40.71041989999999x-74.0024481,40.7106376x-74.0036669,40.711607x-74.00414320000002,40.71196679999999x-74.0043619,40.712142899999996x-74.00442319999999,40.712206200000004x-74.0044588,40.712255299999995x-74.0044942,40.71232450000001x-74.0045008,40.712368600000005x-74.00450859999998,40.7124146x-74.00450409999999,40.712497299999995x-74.0044765,40.71260590000001x-74.0044099,40.7127226x-74.00437769999999,40.71282119999999x-74.0046331,40.7126318x-74.00485059999998,40.7124788x-74.0049167,40.7124347x-74.0052547,40.7122771x-74.0056323,40.7121466x-74.0057898,40.71209029999999x-74.0059794,40.712036700000006x-74.00621570000001,40.711960499999996x-74.00692569999998,40.7117603x-74.0070243,40.711735600000004x-74.00711779999999,40.7117118x-74.00733190000001,40.7117116x-74.0075469,40.71166989999999x-74.00767390000001,40.7116745x-74.0077635,40.7116988x-74.007823,40.7117257x-74.0078875,40.7117675x-74.00799730000001,40.7118889x-74.0081432,40.7119995x-74.00865820000001,40.7113879x-74.00845790000001,40.711247799999995x-74.0073597,40.710587700000005x-74.0073263,40.710550299999994x-74.00607600000001,40.70991779999999x-74.0060473,40.7099466";
		// args[7] = "-overwrite";
		// args[8] = "-no-local";// "-local";

		String inputPath, intermediatePath, outputPath;
		int topk = 0;

		final OperationsParams params = new OperationsParams(
				new GenericOptionsParser(args));

		final Path[] paths = params.getPaths();
		if (paths.length <= 1 && !params.checkInput()) {
			printUsage();
			System.exit(1);
		}
		if (paths.length >= 2 && !params.checkInputOutput()) {
			printUsage();
			System.exit(1);
		}

		inputPath = params.getInputPath().toString();
		outputPath = params.getOutputPath().toString();
		intermediatePath = outputPath + "_temp";
		topk = Integer.parseInt(params.get("k"));

		if (params.get("traj") == null) {
			System.err.println("Trajectory query is missing");
			printUsage();
			System.exit(1);
		}

		// Invoke method to compute the trajectory MBR.
		String rectangle = getTrajectoryRectangle(params.get("traj"));
		System.out.println("rect:" + rectangle);
		params.set("rect", rectangle);

		if (params.get("rect") == null) {
			System.err.println("You must provide a Trajectory Query");
			printUsage();
			System.exit(1);
		}

		if (params.get("interval") == null) {
			System.err.println("Temporal range missing");
			printUsage();
			System.exit(1);
		}

		TextSerializable inObj = params.getShape("shape");
		if (!(inObj instanceof STPoint)) {
			if (!(inObj instanceof STRectangle)) {
				LOG.error("Shape is not instance of STPoint");
				printUsage();
				System.exit(1);
			}

		}

		PriorityQueue<KNNTrajectory.Score> pqueue = new PriorityQueue<KNNTrajectory.Score>();
		HashMap<String, List<STPoint>> overlappedTrajectory = new HashMap<String, List<STPoint>>();
		HashMap<String, STNycTrajectory> overlappedTrajectory_rect = new HashMap<String, STNycTrajectory>();
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		FileSystem fs = FileSystem.get(conf);
		Path checkinterpath = new Path(intermediatePath);
		if (fs.exists(checkinterpath)) {
			fs.delete(checkinterpath);
			fs.mkdirs(checkinterpath);
		} else {
			fs.create(checkinterpath);
		}

		int numTry = 3;
		long t1 = System.currentTimeMillis();
		do {

			if (numTry == 3) {
				params.setOutputPath(intermediatePath);
			} else {
				// invoke method to get the test circle MBR
				CirclePoint queryPoint = getMaximumDistance(params.get("traj"),
						overlappedTrajectory, pqueue);
				Circle range_for_next_iteration = new Circle(queryPoint.p.x,
						queryPoint.p.y, queryPoint.distance * 2);
				Rectangle mbr = range_for_next_iteration.getMBR();
				params.set("rect", mbr.x1 + "," + mbr.y1 + "," + mbr.x2 + ","
						+ mbr.y2);
			}

			// get the overlap data partitions with the given trajectory.
			STRangeQuery.rangeQueryOperation(params);

			// read all the result in the HDFS Build HashMap of trajectory based
			// on TrajId.

			// the second boolean parameter here sets the recursion to true
			RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs
					.listFiles(new Path(intermediatePath), true);
			while (fileStatusListIterator.hasNext()) {
				LocatedFileStatus fileStatus = fileStatusListIterator.next();
				if (fileStatus.isFile()) {
					BufferedReader br = new BufferedReader(
							new InputStreamReader(fs.open(fileStatus.getPath())));
					String line;
					while ((line = br.readLine()) != null) {

						if ((inObj instanceof STPoint)) {

							STNycSTPoints temp = new STNycSTPoints(line);
							if (overlappedTrajectory.containsKey(temp.id)) {
								List<STPoint> plist = overlappedTrajectory
										.get(temp.id);
								plist.add((STPoint) temp);
								overlappedTrajectory.put(temp.id, plist);
							} else {
								// new trajectory inserted.
								List<STPoint> plist = new ArrayList<STPoint>();
								plist.add((STPoint) temp);
								overlappedTrajectory.put(temp.id, plist);
							}
						} else if ((inObj instanceof STRectangle)) {
							STNycTrajectory temp = new STNycTrajectory(line);
							if (overlappedTrajectory.containsKey(temp.id)) {
								STNycTrajectory plist = overlappedTrajectory_rect
										.get(temp.id);
								overlappedTrajectory_rect.put(temp.id, plist);
							} else {

								overlappedTrajectory_rect.put(temp.id, temp);
							}
						}

						// System.out.println(line);
					}
				}
				// do stuff with the file like ...
				// job.addFileToClassPath(fileStatus.getPath());
			}

			// find the top k from the intermediate resutl.
			List<STPoint> query = getTrajectoryQueryPoints(params.get("traj"));
			if ((inObj instanceof STPoint)) {
				Iterator it = overlappedTrajectory.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry entry = (Map.Entry) it.next();
					String id = (String) entry.getKey();
					List<STPoint> traj = (List<STPoint>) entry.getValue();
					Collections.sort(traj);
					DTW similarityfunc = new DTW(query, traj);
					pqueue.add(new Score(id, similarityfunc.getDistance()));
				}
			} else if ((inObj instanceof STRectangle)) {
				Iterator it = overlappedTrajectory_rect.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry entry = (Map.Entry) it.next();
					String id = (String) entry.getKey();
					STNycTrajectory traj_rect = (STNycTrajectory) entry
							.getValue();
					DTW similarityfunc = new DTW(query,
							traj_rect.getPointsList());
					pqueue.add(new Score(id, similarityfunc.getDistance()));
				}
			}

			numTry--;

		} while (pqueue.size() < topk && numTry >= 0);
		long t2 = System.currentTimeMillis();
		System.out.println("Time for " + topk + " jobs is " + (t2 - t1)
				+ " millis");
		System.out.println("Total iterations: " + Math.abs(numTry - 3));
		// check if the retrieved top-k is equal to the requested k.
		System.out.println("found top-" + pqueue.size() + "\tTryNum:" + numTry);
		if (pqueue.size() >= topk) {
			// write the result to HDFS
			Path outputHDFS = new Path(outputPath);
//			if (fs.exists(outputHDFS)) {
//				fs.delete(outputHDFS);
//			}
//			FSDataOutputStream fout = fs.create(outputHDFS);
			for (int i = 0; i < topk; i++) {
				Score temp = pqueue.poll();
				System.out.println(temp.id + "\t*Similarity Score: "
						+ temp.score);
//				if ((inObj instanceof STPoint)) {
//					List<STPoint> trajectory = overlappedTrajectory
//							.get(temp.id);
//					Collections.sort(trajectory);
//					for (STPoint p : trajectory) {
//						String text = p.x + "," + p.y + ";";
//						text.replace("\00", "");
//						fout.writeBytes(text);
//
//					}
//				} else if ((inObj instanceof STRectangle)) {
//					STNycTrajectory trajectory = overlappedTrajectory_rect
//							.get(temp.id);
//					String text = trajectory.toString();
//					text.replace("\00", "");
//					fout.writeBytes(text);
//				}
//				fout.writeBytes("\n");
			}

//			fout.close();
		}

		// remove intermediate file.
//		fs.delete(new Path(intermediatePath));

	}

	private static class Score implements Comparable<Score> {
		String id;
		double score;

		public Score() {
		}

		public Score(String id, double score) {
			this.id = id;
			this.score = score;
		}

		@Override
		public int compareTo(Score o) {
			return this.score < o.score ? -1 : (this.score > o.score ? +1 : 0);
		}

		@Override
		public String toString() {
			return id + " -Similarity Score:" + score;
		}
	}

}
