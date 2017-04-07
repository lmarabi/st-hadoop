package edu.umn.cs.sthadoop.operations;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.sthadoop.core.STPoint;

public class testsjoin {

	public static void main(String[] args) throws Exception {
		String fileName = "/home/louai/nyc-taxi/candidatebuckets/yellowIndex/00000-2015-01-02/part-m-00000";
		BufferedReader br =  new BufferedReader(new FileReader(fileName));
		String line;
//		double x1 = -180;
//		double y1 = -90;
//		double x2 = 180;
//		double y2 = 90;
//		double interval = 0.01;
//		int column = (int)((x2 - x1)/ interval) +1;
//		int row = (int)((y2 - y1)/ interval) +1;
//		GridInfo grid = new GridInfo(-180, -90, 180, 90);
//		IntWritable cellId = new IntWritable();
		ArrayList<STPoint> shapes = new ArrayList<STPoint>();
		while ((line = br.readLine()) != null) {
			System.out.println(line);
			
			STPoint shape = new STPoint();
			shape.fromText(new Text(line));
			shapes.add(shape);
//			Rectangle mbr = shape.getMBR();
//			int cellID = grid.getOverlappingCell(shape.x, shape.y);
//			// find row column 
//			int columnID = (int)((shape.x - x1)/ interval);
//			int rowID = (int)((shape.y - y1)/ interval);
//			System.out.println("ID("+columnID+","+rowID+") ------>"+shape.toString());
//			
//			
//			java.awt.Rectangle cells = grid.getOverlappingCells(shape.getMBR());
//			for (int col = cells.x; col < cells.x + cells.width; col++) {
//		          for (int rowd = cells.y; rowd < cells.y + cells.height; rowd++) {
//		            cellId.set(rowd * grid.columns + col + 1);
//		            System.out.println("CellID("+cellId.toString()+") ------>"+shape.toString());
//		          }
//		        }
			
		}//endwhile
		int distance = 1; 
		String time = "hour";
		int interval = 2;		
		// find nested join result. 
		for(STPoint point : shapes){
			for(STPoint x : shapes){
				if(point.equals(x))
					continue;
				if(point.distanceTo(x) <= distance && 
						getTimeDistance(point.time, x.time, time, interval)){
					System.out.println("<"+point.toString()+"\t"+x.toString()+">");
				}
			}
		}

	}
	
	static private boolean getTimeDistance(String time1 , String time2, String flag, int interval) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		boolean result = false;
//		try {
//			Date d1 = format.parse(time1);
//			Date d2 = format.parse(time2);
//
//			//in milliseconds
//			long diff = d2.getTime() - d1.getTime();
//			
//			switch (flag) {
//			case "day":
//				if(interval <= (diff / (24 * 60 * 60 * 1000)))
//					result = true;
//				break;
//			case "hour":
//				if(interval <= (diff / (60 * 60 * 1000) % 24))
//					result = true;
//				break;
//			case "minute":
//				if(interval <= (diff / (60 * 1000) % 60))
//					result = true;
//				break;
//			case "second":
//				if(interval <= (diff / 1000 % 60))
//					result = true;
//				break;
//
//			default:
//				break;
//			}
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//		}

		return result;
	}

}
