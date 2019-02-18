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
		// args[0] =
		// "/export/scratch/mntgData/geolifeGPS/geolife_Trajectories_1.3/HDFS/index_geolife";
		// args[1] =
		// "/export/scratch/mntgData/geolifeGPS/geolife_Trajectories_1.3/HDFS/knn-dis-result";
		// args[2] = "shape:edu.umn.cs.sthadoop.trajectory.GeolifeTrajectory";
		// args[3] = "interval:2008-05-01,2008-05-30";
		// args[4] = "time:month";
		// args[5] = "k:100";
		// args[6] =
		// "traj:39.9119983,116.606835x39.9119783,116.6065483x39.9119599,116.6062649x39.9119416,116.6059899x39.9119233,116.6057282x39.9118999,116.6054783x39.9118849,116.6052366x39.9118666,116.6050099x39.91185,116.604775x39.9118299,116.604525x39.9118049,116.6042649x39.91177,116.6040166x39.9117516,116.6037583x39.9117349,116.6035066x39.9117199,116.6032666x39.9117083,116.6030232x39.9117,116.6027566x39.91128,116.5969383x39.9112583,116.5966766x39.9112383,116.5964232x39.9112149,116.5961699x39.9111933,116.5959249x39.9111716,116.5956883";
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
		HashMap<String, List<STNycTrajectory>> overlappedTrajectory_rect = new HashMap<String, List<STNycTrajectory>>();
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		FileSystem fs = FileSystem.get(conf);
		Path checkinterpath = new Path(intermediatePath);
		if (fs.exists(checkinterpath)) {
			fs.delete(checkinterpath);
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
						}
						else if((inObj instanceof STRectangle)){
							STNycTrajectory temp = new STNycTrajectory(line);
							if (overlappedTrajectory.containsKey(temp.id)) {
								List<STNycTrajectory> plist = overlappedTrajectory_rect
										.get(temp.id);
								plist.add((STNycTrajectory) temp);
								overlappedTrajectory_rect.put(temp.id, plist);
							} else {
								// new trajectory inserted.
								List<STNycTrajectory> plist = new ArrayList<STNycTrajectory>();
								plist.add((STNycTrajectory) temp);
								overlappedTrajectory_rect.put(temp.id, plist);
							}
						}

						// System.out.println(line);
					}
				}
				// do stuff with the file like ...
				// job.addFileToClassPath(fileStatus.getPath());
			}
			// remove intermediate file.
			fs.delete(new Path(intermediatePath));
			// find the top k from the intermediate resutl.
			List<STPoint> query = getTrajectoryQueryPoints(params.get("traj"));

			Iterator it = overlappedTrajectory.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry entry = (Map.Entry) it.next();
				String id = (String) entry.getKey();
				List<STPoint> traj = (List<STPoint>) entry.getValue();
				Collections.sort(traj);
				DTW similarityfunc = new DTW(query, traj);
				pqueue.add(new Score(id, similarityfunc.getDistance()));
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
			if (fs.exists(outputHDFS)) {
				fs.delete(outputHDFS);
			}
			FSDataOutputStream fout = fs.create(outputHDFS);
			for (int i = 0; i < topk; i++) {
				Score temp = pqueue.poll();
				System.out.println(temp.id + "\tSimilarity Score: "
						+ temp.score);
				List<STPoint> trajectory = overlappedTrajectory.get(temp.id);
				Collections.sort(trajectory);
				for (STPoint p : trajectory) {
					String text = p.x + "," + p.y + ";";
					text.replace("\00", "");
					fout.writeBytes(text);

				}
				fout.writeBytes("\n");
			}

			fout.close();
		}

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
