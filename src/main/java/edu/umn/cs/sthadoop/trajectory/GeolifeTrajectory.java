package edu.umn.cs.sthadoop.trajectory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.sthadoop.core.STPoint;

/**
 * A data type used for GeoLife trajectories dataset.
 *  Metadata: uerID-Tripid,0,Altitude,FractionNumber,DateTime,Longitude,Latitude
	Example: 102-20111023101803,0,0,40839.4303703704,2011-10-23 10:19:44,39.9067083333333,116.429903333333
 * @author Louai Alarabi
 */
public class GeolifeTrajectory extends STPoint {
	private static final Log LOG = LogFactory.getLog(GeolifeTrajectory.class);
	public String id;
	public String flag; 
	public String altitude;
	public String fraction;
	public String datetime;
	public String longitude;
	public String latitude;
	

	public GeolifeTrajectory() {
		// TODO Auto-generated constructor stub
	}

	public GeolifeTrajectory(String text) throws ParseException {
		String[] list = text.toString().split(",");
		id = list[0];
		flag = list[1];
		altitude = list[2];
		fraction = list[3];
		datetime = list[4];
		longitude = list[5];
		latitude = list[6];
		super.fromText(new Text(datetime + "," + longitude + "," + latitude));

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);
		out.writeUTF(flag);
		out.writeUTF(altitude);
		out.writeUTF(fraction);
		super.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readUTF();
		flag = in.readUTF();
		altitude = in.readUTF();
		fraction = in.readUTF(); 
		super.readFields(in);

	}

	@Override
	public Text toText(Text text) {
		byte[] separator = new String(",").getBytes();
		text.append(id.getBytes(), 0, id.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(flag.getBytes(), 0, flag.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(altitude.getBytes(), 0, altitude.getBytes().length);
		text.append(separator, 0, separator.length);
		text.append(fraction.getBytes(), 0, fraction.getBytes().length);
		text.append(separator, 0, separator.length);
		super.toText(text);
		return text;
	}

	@Override
	public void fromText(Text text) {
		String[] list = text.toString().split(",");
		id = list[0];
		flag = list[1];
		altitude = list[2];
		fraction = list[3];
		datetime = list[4];
		longitude = list[5];
		latitude = list[6];
		super.fromText(new Text(datetime + "," +longitude + "," + latitude));

	}

	@Override
	public GeolifeTrajectory clone() {
		GeolifeTrajectory c = new GeolifeTrajectory();
		c.id = this.id;
		c.flag = this.flag;
		c.altitude = this.altitude;
		c.fraction = this.fraction;
		c.time = this.datetime;
		c.time = this.time;
		c.longitude = this.longitude;
		c.latitude = this.latitude;
		c.x = this.x;
		c.y = this.y;
		return c;
	}

	@Override
	public String toString() {
		return "GeolifeTrajectory: (" + id + "," + flag + "," + altitude + "," + fraction + ","
				+ time + ", "+ x + "," + y +  ")";
	}

	public static void main(String[] args) {
		String temp = "102-20111023101803,0,0,40839.4303703704,2011-10-23 10:19:44,39.9067083333333,116.429903333333";

		GeolifeTrajectory point = new GeolifeTrajectory();
		point.fromText(new Text(temp));
		STPoint point3d = (STPoint) point;
		System.out.println(point.time);
		System.out.println(point3d.time);
		System.out.println(point.x);
		System.out.println(point3d.x);
		System.out.println(point.y);
		System.out.println(point3d.y);

		// Test casting from 3D to 2D shape.
		Point point2D = (Point) point;
		Text txt = new Text();
		point.toText(txt);
		System.out.println("Point : " + point.toString());
		System.out.println("Point3D : " + point3d.toString());
		System.out.println("Point2D : " + point2D.toString());

	}

}
