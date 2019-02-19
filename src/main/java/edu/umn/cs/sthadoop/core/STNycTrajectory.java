package edu.umn.cs.sthadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Rectangle;

/**
 * A data type used to represents Trajectory data. This class support both csv
 * with pickup_time only, or with both pickup and dropoff times. #csv = id,
 * pickupdate_time , [dropoff_time]+ , [longitude & latitude, longitude &
 * latitude]*
 * 
 * @author Louai Alarabi
 */

public class STNycTrajectory extends STRectangle {
	public String id;
	public String startTime;
	public String endTime;
	public String points;

	public STNycTrajectory() {
		// TODO Auto-generated constructor stub
		super("", 0, 0, 0, 0);
	}

	public STNycTrajectory(String text) throws ParseException {
		this.fromText(new Text(text));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(id);
		out.writeUTF(startTime);
		out.writeUTF(endTime);
		out.writeUTF(points);
		super.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id = in.readUTF();
		startTime = in.readUTF();
		endTime = in.readUTF();
		points = in.readUTF();
		super.readFields(in);
	}

	@Override
	public Text toText(Text text) {
		// TODO Auto-generated method stub
		byte[] separator = new String(",").getBytes();
		text.append(id.getBytes(), 0, id.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(startTime.getBytes(), 0, startTime.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(endTime.getBytes(), 0, endTime.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(points.getBytes(), 0, points.getBytes().length);
		text.append(separator, 0, separator.length);
		return super.toText(text);
	}
	
	public List<STPoint> getPointsList(){
		List<STPoint> list = new ArrayList<STPoint>();
		String token[] = points.split(",");
		for( String p: token){ 
			STPoint temp = new STPoint();
			temp.fromText(new Text("0000-00-00,"+ p.replace("&", ",") ));
			list.add(temp);
		}
		return list;
	}

	@Override
	public void fromText(Text text) {
		// TODO Auto-generated method stub
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		double temp = 0;
		String[] list = text.toString().split(",");
		id = list[0];
		startTime = list[1];
		time = startTime;
		int incremmental;
		try {
			endTime = (sdf.parse(list[2]) != null) ? list[2] : list[1];
			incremmental = 3;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			endTime = list[1];
			incremmental = 2;
		}
		double tempx, tempy;
		StringBuilder fullTraj = new StringBuilder();
		String[] startpoint = list[incremmental].split("&");
		tempx = Double.parseDouble(startpoint[0]);
		tempy = Double.parseDouble(startpoint[1]);
		this.x1 = tempx;
		this.y1 = tempy;
		this.x2 = tempx;
		this.y2 = tempy;
		for (int i = incremmental; i < list.length; i++) {
			String[] point = list[i].split("&");
			
			if (point.length == 2) {
				if ((i + 1) == list.length) {
					fullTraj.append(list[i]);
				} else {
					if(list[i+1].contains("&")){
						fullTraj.append(list[i] + ",");		
					}else
					{
						fullTraj.append(list[i]);
						break;
					}
					
					
				}

				tempx = Double.parseDouble(point[0]);
				tempy = Double.parseDouble(point[1]);
				this.x1 = (tempx < x1) ? tempx : x1;
				this.x2 = (tempx > x2) ? tempx : x2;
				this.y1 = (tempy < y1) ? tempy : y1;
				this.y2 = (tempy > y2) ? tempy : y2;
			}

		}
		points = fullTraj.toString();
	}

	@Override
	public STNycTrajectory clone() {
		// TODO Auto-generated method stub
		STNycTrajectory c = new STNycTrajectory();
		c.id = this.id;
		c.points = this.points;
		c.startTime = this.startTime;
		c.time = this.time;
		c.endTime = this.endTime;
		c.x1 = this.x1;
		c.x2 = this.x2;
		c.y1 = this.y1;
		c.y2 = this.y2;
		return c;
	}
	
	

	public static void main(String[] args) throws Exception {
		String traj = "yellowID_2_91735066,2015-10-09 12:29:30,-73.95378875732422&40.78493118286133,-73.95453640000002&40.7840339,-73.9549902&40.7834104,-73.9554459&40.78278420000001,-73.95590179999999&40.782157700000006,-73.95636329999999&40.78152370000001,-73.9568157&40.780901899999996,-73.95731020000001&40.7802224,-73.9578075&40.7795391,-73.9582675&40.7789071,-73.95872969999999&40.7782719,-73.9591925&40.7776358,-73.9596574&40.7769971,-73.9601187&40.776363200000006,-73.96062330000001&40.77566970000001,-73.96112159999998&40.77498489999999,-73.96157190000001&40.774366099999995,-73.96203159999999&40.773734299999994,-73.9624945&40.7730982,-73.96296229999999&40.7724554,-73.9634226&40.7718227,-73.96391650000001&40.771144,-73.964415&40.7704589,-73.9648661&40.769838899999996,-73.9653277&40.76920450000001,-73.96577979999999&40.76858310000001,-73.966239&40.7679521,-73.96669440000001&40.7673262,-73.96714790000001&40.7667029,-73.96760390000001&40.7660762,-73.96806020000001&40.765449000000004,-73.96851570000001&40.76482299999999,-73.9689701&40.7641984,-73.96942609999999&40.76357170000001,-73.96987659999999&40.762952399999996,-73.9703295&40.7623299,-73.9708284&40.7616441,-73.9713198&40.76096879999999,-73.97177540000001&40.76034249999999,-73.9722305&40.75971700000001,-73.9726864&40.7590902,-73.9731439&40.758461399999995,-73.9735977&40.7578377,-73.97405390000002&40.75721060000001,-73.9745087&40.7565855,-73.9749659&40.75595690000001,-73.97475390000002&40.755868,-73.9741137&40.75559729999999,-73.973276&40.755243,-73.971677&40.75457";
		//String traj = "yellowID_2_91735066,2015-10-09 12:29:30,2015-10-09 12:29:30,-73.95378875732422&40.78493118286133,-73.95453640000002&40.7840339,-73.9549902&40.7834104,-73.9554459&40.78278420000001,-73.95590179999999&40.782157700000006,-73.95636329999999&40.78152370000001,-73.9568157&40.780901899999996,-73.95731020000001&40.7802224,-73.9578075&40.7795391,-73.9582675&40.7789071,-73.95872969999999&40.7782719,-73.9591925&40.7776358,-73.9596574&40.7769971,-73.9601187&40.776363200000006,-73.96062330000001&40.77566970000001,-73.96112159999998&40.77498489999999,-73.96157190000001&40.774366099999995,-73.96203159999999&40.773734299999994,-73.9624945&40.7730982,-73.96296229999999&40.7724554,-73.9634226&40.7718227,-73.96391650000001&40.771144,-73.964415&40.7704589,-73.9648661&40.769838899999996,-73.9653277&40.76920450000001,-73.96577979999999&40.76858310000001,-73.966239&40.7679521,-73.96669440000001&40.7673262,-73.96714790000001&40.7667029,-73.96760390000001&40.7660762,-73.96806020000001&40.765449000000004,-73.96851570000001&40.76482299999999,-73.9689701&40.7641984,-73.96942609999999&40.76357170000001,-73.96987659999999&40.762952399999996,-73.9703295&40.7623299,-73.9708284&40.7616441,-73.9713198&40.76096879999999,-73.97177540000001&40.76034249999999,-73.9722305&40.75971700000001,-73.9726864&40.7590902,-73.9731439&40.758461399999995,-73.9735977&40.7578377,-73.97405390000002&40.75721060000001,-73.9745087&40.7565855,-73.9749659&40.75595690000001,-73.97475390000002&40.755868,-73.9741137&40.75559729999999,-73.973276&40.755243,-73.971677&40.75457,2015-10-09 12:29:30,-73.9749659,40.75457,-73.95378875732422,40.78493118286133";
		STNycTrajectory trajshape = new STNycTrajectory(traj);
		System.out.println("ID:" + trajshape.id);
		System.out.println("starttime:" + trajshape.startTime);
		System.out.println("endtime:" + trajshape.endTime);
		System.out.println("Rectangle:" + trajshape.toString());
		System.out.println("points:" + trajshape.points);
		System.out.println("MBR(" + trajshape.x1 + "," + trajshape.y1 + "," + trajshape.x2 + "," + trajshape.y2 + ")");
		System.out.println("==========");
		System.out.println("To Text ");
		Text text = new Text();
		trajshape.toText(text);
		System.out.println(text.toString());
		STRectangle rect = (STRectangle) trajshape;
		System.out.println("==========");
		text = new Text();
		System.out.println(rect.toString());
		System.out.println(rect.toText(text));
		STNycTrajectory fromTexttraj = new STNycTrajectory();
		fromTexttraj.fromText(text);
		Text text2 = new Text();
		trajshape.toText(text2);
		System.out.println(text2.toString());

	}

}
