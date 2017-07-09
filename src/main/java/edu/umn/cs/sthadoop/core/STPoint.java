package edu.umn.cs.sthadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

public class STPoint extends Point {
	private static final Log LOG = LogFactory.getLog(STPoint.class);
	public String time;

	public STPoint() {
		// TODO Auto-generated constructor stub
	}

	public STPoint(String text) throws ParseException {
		this.fromText(new Text(text));
	}

	public void set(double x, double y, String t) {
		this.time = t;
		super.set(x, y);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(time);
		super.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		time = in.readUTF();
		super.readFields(in);

	}

	@Override
	public Text toText(Text text) {
		byte[] separator = new String(",").getBytes();
		text.append(time.getBytes(), 0, time.getBytes().length);
		text.append(separator, 0,separator.length);
		super.toText(text);
		return text;
	}

	@Override
	public void fromText(Text text) {
		String[] list = text.toString().split(",");
		time = list[0];
		super.fromText(new Text(list[1] + "," + list[2]));
	}

	@Override
	public STPoint clone() {
		STPoint c = new STPoint();
		c.time = this.time;
		c.x = this.x;
		c.y = this.y;
		return c;
	}
	
	@Override
	public String toString() {
		return "STPoint: ("+x+","+y+","+time+")";
	}

}
