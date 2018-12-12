package edu.umn.cs.sthadoop.core;

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
 * A data type used for NYC trajectory as set of points. 
	Example: id, time, x, y. 
 * @author Louai Alarabi
 */
public class STNycSTPoints extends STPoint {
	private static final Log LOG = LogFactory.getLog(STNycSTPoints.class);
	public String id;
	public String datetime;
	public String longitude;
	public String latitude;
	

	public STNycSTPoints() {
		// TODO Auto-generated constructor stub
	}

	public STNycSTPoints(String text) throws ParseException {
		String[] list = text.toString().split(",");
		id = list[0];
		datetime = list[1];
		longitude = list[2];
		latitude = list[3];
		super.fromText(new Text(datetime + "," + longitude + "," + latitude));

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);
		super.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readUTF();
		super.readFields(in);

	}

	@Override
	public Text toText(Text text) {
		byte[] separator = new String(",").getBytes();
		text.append(id.getBytes(), 0, id.getBytes().length);
		text.append(separator, 0, separator.length);
		super.toText(text);
		return text;
	}

	@Override
	public void fromText(Text text) {
		String[] list = text.toString().split(",");
		id = list[0];
		datetime = list[1];
		longitude = list[2];
		latitude = list[3];
		super.fromText(new Text(datetime + "," +longitude + "," + latitude));

	}

	@Override
	public STNycSTPoints clone() {
		STNycSTPoints c = new STNycSTPoints();
		c.id = this.id;
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
		return "STNycSTPoints: (" + id + "," + time + ", "+ x + "," + y +  ")";
	}

	public static void main(String[] args) {
		String temp = "102-20111023101803,2011-10-23 10:19:44,39.9067083333333,116.429903333333";

		STNycSTPoints point = new STNycSTPoints();
		point.fromText(new Text(temp));
		STPoint point3d = (STPoint) point;
		System.out.println(point.time);
		System.out.println(point3d.time);
		System.out.println(point.x);
		System.out.println(point3d.x);
		System.out.println(point.y);
		System.out.println(point3d.y);
		
		if (!(point instanceof STPoint)) {
			LOG.error("Shape is not instance of STPoint");
			return;
		}
		
		// Test casting from 3D to 2D shape.
		Point point2D = (Point) point;
		Text txt = new Text();
		point.toText(txt);
		System.out.println("Point : " + point.toString());
		System.out.println("Point3D : " + point3d.toString());
		System.out.println("Point2D : " + point2D.toString());

	}

}
