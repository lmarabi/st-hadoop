package edu.umn.cs.sthadoop.trajectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.sthadoop.core.STPoint;

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
	 * @param trajectory
	 * @return
	 */
	private static String getTrajectoryRectangle(String trajectory){
		String [] trajectoryPoints = trajectory.split(";");
		double x1 = Double.parseDouble(trajectoryPoints[0].split(",")[0]);
		double y1 = Double.parseDouble(trajectoryPoints[0].split(",")[1]);
		double x2 = x1;
		double y2 = y1;
		double tempDouble;
		for( String p : trajectoryPoints){
			String [] point = p.split(",");
			// get the minMax of x point
			tempDouble = Double.parseDouble(point[0]);
			if(tempDouble <= x1)
				x1 = tempDouble;
			if(tempDouble >= x2)
				x2 = tempDouble;
			// get the minMax of y.
			tempDouble = Double.parseDouble(point[1]);
			if(tempDouble <= y1)
				y1 = tempDouble;
			if(tempDouble >= y2)
				y2 = tempDouble;
		}
		return  Double.toString(x1)+ ","+ Double.toString(y1) + "," + Double.toString(x2) + "," + Double.toString(y2);
		
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
		
		
		
		args = new String[9];
		args[0] = "/export/scratch/mntgData/geolifeGPS/geolife_Trajectories_1.3/HDFS/index_geolife";
		args[1] = "/export/scratch/mntgData/geolifeGPS/geolife_Trajectories_1.3/HDFS/knn-dis-result";
		args[2] = "shape:edu.umn.cs.sthadoop.trajectory.GeolifeTrajectory";
		args[3] = "interval:2008-05-01,2008-05-30";
		args[4] = "time:month";
		args[5] = "k:1";
		args[6] = "traj:39.9119983,116.606835;39.9119783,116.6065483;39.9119599,116.6062649;39.9119416,116.6059899;39.9119233,116.6057282;39.9118999,116.6054783;39.9118849,116.6052366;39.9118666,116.6050099;39.91185,116.604775;39.9118299,116.604525;39.9118049,116.6042649;39.91177,116.6040166;39.9117516,116.6037583;39.9117349,116.6035066;39.9117199,116.6032666;39.9117083,116.6030232;39.9117,116.6027566;39.91128,116.5969383;39.9112583,116.5966766;39.9112383,116.5964232;39.9112149,116.5961699;39.9111933,116.5959249;39.9111716,116.5956883";
		args[7] = "-overwrite";
		args[8] =  "-no-local";//"-local";
		
		String inputPath,intermediatePath,outputPath; 
		
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args));

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
		intermediatePath = outputPath+"_temp";
		
		if(params.get("traj") == null){
			System.err.println("Trajectory query is missing");
			printUsage();
			System.exit(1);
		}
		
		// Invoke method to compute the trajectory MBR. 
		String rectangle = getTrajectoryRectangle(params.get("traj"));
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
			LOG.error("Shape is not instance of STPoint");
			printUsage();
			System.exit(1);
		}
		
		// get the overlap data partitions with the given trajectory.
		String[] targs = new String[9];
		targs[0] = inputPath;
		targs[1] = intermediatePath;
		targs[2] = "shape:" + params.get("shape");
		targs[3] = "interval:" + params.get("interval");
		targs[4] = "time:" + params.get("time");
		targs[5] = "k:" + params.get("k");
		targs[6] = "traj:" + params.get("traj");
		targs[7] = "-overwrite";
		targs[8] =  "-no-local";//"-local";
		TrajectoryOverlap.main(targs);
	
		
		// read all the result in the HDFS Build HashMap of trajectory based on TrajId.  
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf);
	    FileSystem fs = FileSystem.get(conf);
	    HashMap<String, STPoint> overlappedTrajectory = new HashMap<String, STPoint>();
	    //the second boolean parameter here sets the recursion to true
	    RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(
	            new Path(intermediatePath), true);
	    while(fileStatusListIterator.hasNext()){
	        LocatedFileStatus fileStatus = fileStatusListIterator.next();
	        if(fileStatus.isFile()){
	        	BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
	            String line;
	            line=br.readLine();
	            while (line != null){
	            	GeolifeTrajectory temp = new GeolifeTrajectory(line);
	            	overlappedTrajectory.put(temp.id, (STPoint)temp);
	                System.out.println(line);
	                line=br.readLine();
	            }
	        }
	        //do stuff with the file like ...
	        //job.addFileToClassPath(fileStatus.getPath());
	    }
		
		
		
	}

}
