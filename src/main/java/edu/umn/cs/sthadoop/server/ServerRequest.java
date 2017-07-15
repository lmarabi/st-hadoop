/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umn.cs.sthadoop.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.sthadoop.core.STPoint;
import edu.umn.cs.sthadoop.core.STpointsTweets;
import edu.umn.cs.sthadoop.operations.STRangeQuery;

/**
 *
 * @author Louai Alarabi
 */
/**
 * @author louai
 *
 */
public class ServerRequest {

	private Commons config;
	private queryShape shape;
	private queryoperation operation;
	private String x1;
	private String y1;
	private String x2;
	private String y2;
	private String t1;
	private String t2;
	private MBR rect;
	//Attributes members for the answer. 
	List<Partition> stPartitions;
	TopResults finalResult; 
	
	
	public ServerRequest() throws FileNotFoundException, IOException, ParseException {
		// load the configuration file.
		this.config = new Commons();
		this.stPartitions = new ArrayList<>();
		this.finalResult = new TopResults(900);
	}
	
	public String getX1() {
		return x1;
	}

	public void setX1(String x1) {
		this.x1 = x1;
	}

	public String getY1() {
		return y1;
	}

	public void setY1(String y1) {
		this.y1 = y1;
	}

	public String getX2() {
		return x2;
	}

	public void setX2(String x2) {
		this.x2 = x2;
	}

	public String getY2() {
		return y2;
	}

	public void setY2(String y2) {
		this.y2 = y2;
	}

	public String getT1() {
		return t1;
	}

	public void setT1(String t1) {
		this.t1 = t1;
	}

	public String getT2() {
		return t2;
	}

	public void setT2(String t2) {
		this.t2 = t2;
	}

	public MBR getRect() {
		return rect;
	}
	
	/**
	 * Get the final result from the priority queue 
	 * @return
	 */
	public List<Point> getFinalResult(){
		List<Point> result = new ArrayList<>();
		while(finalResult.size() > 0){
			result.add(finalResult.pop());
		}
		return result;
	}
	
	public void setRect(String x1, String y1, String x2, String y2) {
		this.x1 = x1;
		this.y1 = y1;
		this.x2 = x2;
		this.y2 = y2;
		this.rect = new MBR(new Point(x1, y1),new Point(x2, y2)) ;
	}

	public enum queryShape {

		stpoint, twitter
	};

	public enum queryoperation {

		rq, join, knn
	};

	public queryShape getShape() {
		return shape;
	}

	public void setShape(queryShape shape) {
		this.shape = shape;
	}

	public queryoperation getOperation() {
		return operation;
	}

	public void setOperation(queryoperation operation) {
		this.operation = operation;
	}

	

	public void executeQuery() throws Exception {
		if (operation.equals(queryoperation.rq)) {
			executeMapReduceRQ();
		} else if (operation.equals(queryoperation.join)) {
			// executeRQ();
		} else if (operation.equals(queryoperation.knn)) {
			// executeRQ();
		} else {
			System.out.println("no selected operation!");
		}
	}

	/**
	 * This method invok the ST-Hadoop operations and only write the result to
	 * the outputfile.
	 * 
	 * @throws Exception
	 */
	public void executeMapReduceRQ() throws Exception {
		String[] args = new String[5];
		args[0] = Commons.getQueryIndex();
		File file = new File(Commons.getQueryResult());
		if (file.exists()) {
			file.delete();
		}
		args[1] = Commons.getQueryResult();
		if (shape.equals(queryShape.twitter)) {
			args[2] = "shape:edu.umn.cs.sthadoop.core.STpointsTweets";
		} else {
			args[2] = "shape:edu.umn.cs.sthadoop.core.STPoint";
		}
		args[3] = "rect:" + x1 + "," + y1 + "," + x2 + "," + y2;
		args[4] = "interval:" + t1 + "," + t2;
		STRangeQuery.main(args);

	}
	
	
	/**
	 * This method execute the range query from the disk. 
	 * @param mbr
	 * @param path
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public long executeRangeQuery(){
		long count =0;
		for(Partition part : this.stPartitions){
			try {
				count += smartQuery(part);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return count;
	}


/**
 * This method Write the Final result .
 *
 * @param f
 * @param out
 *            Streamer
 * @throws FileNotFoundException
 * @throws IOException
 */
private synchronized int smartQuery(Partition part)
		throws FileNotFoundException, IOException, ParseException {
	BufferedReader reader;
	// FileInputStream fin = new FileInputStream(part.getPartition());
	// BufferedInputStream bis = new BufferedInputStream(fin);
	// CompressorInputStream input = new
	// CompressorStreamFactory().createCompressorInputStream(bis);
	// BufferedReader reader = new BufferedReader(new
	// InputStreamReader(input,"UTF-8"));
	reader = new BufferedReader(new InputStreamReader(new FileInputStream(
			part.getPartition()), "UTF-8"));
	String line;
	int count = 0;
	// get the range file
	while ((line = reader.readLine()) != null) {
		// Here rather than wrting to local storage you can pass it
		// right away to Visualization team.
		if (this.shape.equals(queryShape.stpoint)) {
			STPoint obj = new STPoint(line);
			Point point = new Point(obj.x, obj.y);
			if (this.rect.insideMBR(point)) {
				finalResult.insert(point);
				count++;
			}
		}else if(this.shape.equals(queryShape.twitter)){
			STpointsTweets obj = new STpointsTweets(line);
			Point point = new Point(obj.x, obj.y);
			if (this.rect.insideMBR(point)) {
				finalResult.insert(point);
				count++;
			}
		}else{
			System.out.println("unidentified shape");
		}

	}
	reader.close();
	return count;
}
	
	
	/**
	 * This method to return the list of selected Spatial/Spatio-Temporal/Temporal Range.
	 * @param level = if pass null then spatio-temporal selected; otherwise choose one [day-week-month-year] 
	 * @return
	 * @throws Exception
	 */
	public List<Partition> getQueryPartitions(String level) throws Exception{
		String[] args;
		if(level == null){
			args = new String[5];
			args[0] = Commons.getQueryIndex();
			File file = new File(Commons.getQueryResult());
			if (file.exists()) {
				file.delete();
			}
			args[1] = Commons.getQueryResult();
			if (shape.equals(queryShape.twitter)) {
				args[2] = "shape:edu.umn.cs.sthadoop.core.STpointsTweets";
			} else {
				args[2] = "shape:edu.umn.cs.sthadoop.core.STPoint";
			}
			args[3] = "rect:" + x1 + "," + y1 + "," + x2 + "," + y2;
			args[4] = "interval:" + t1 + "," + t2;
		}else{
			args = new String[6];
			args[0] = Commons.getQueryIndex();
			File file = new File(Commons.getQueryResult());
			if (file.exists()) {
				file.delete();
			}
			args[1] = Commons.getQueryResult();
			if (shape.equals(queryShape.twitter)) {
				args[2] = "shape:edu.umn.cs.sthadoop.core.STpointsTweets";
			} else {
				args[2] = "shape:edu.umn.cs.sthadoop.core.STPoint";
			}
			args[3] = "rect:" + x1 + "," + y1 + "," + x2 + "," + y2;
			args[4] = "interval:" + t1 + "," + t2;
			args[5] = "time:"+level;
		}
		
	    OperationsParams params = new OperationsParams(new GenericOptionsParser(args), false);
	    // get the temporal range from ST-Hadoop index. 
		List<Path> paths = STRangeQuery.getIndexedSlices(params);
		// get the exact partitions.
		for(Path p : paths){
			this.stPartitions.addAll(ReadMaster(p));
		}
		return stPartitions;
	}

	/**
	 * This Method read all the files in the data directory and fetch only the
	 * Intersect files
	 *
	 * @param maxLat
	 * @param minLat
	 * @param maxLon
	 * @param minLon
	 * @param path
	 * @return
	 */
	public List<Partition> ReadMaster(Path dir) {
		File master;
		String path = dir.getParent()+"/"+dir.getName();
		List<Partition> result = new ArrayList<Partition>();
		// check the master files with the index used at the backend
		master = new File(path + "/_master.quadtree");
		if (!master.exists()) {
			master = new File(path + "/_master.str");
			if (!master.exists()) {
				master = new File(path + "/_master.str+");
				if (!master.exists()) {
					master = new File(path + "/_master.grid");
				}
			}
		}

		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(master));

			// FileInputStream fin = new FileInputStream(master);
			// BufferedInputStream bis = new BufferedInputStream(fin);
			// CompressorInputStream input = new
			// CompressorStreamFactory().createCompressorInputStream(bis);
			// BufferedReader reader = new BufferedReader(new
			// InputStreamReader(input, "UTF-8"));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] temp = line.split(",");
				// The file has the following format as Aggreed with the
				// interface
				// between hadoop and this program
				// #filenumber,minLat,minLon,maxLat,maxLon
				// 0,minLon,MinLat,MaxLon,MaxLat,Filename
				if (temp.length == 8) {
					Partition part = new Partition(line, path, dir.getName());
					// System.out.println(part.getPartition().getName()+"\t"+part.getArea().toWKT());
					if (rect.Intersect(part.getArea())) {
						result.add(part);
					}
				}
			}
			reader.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

}
