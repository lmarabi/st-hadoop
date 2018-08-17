package edu.umn.cs.sthadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Rectangle;

/**
 * A data type used to represents Trajectory data.
 * @author Louai Alarabi
 */

public class STTrajectory extends Rectangle{
	String id;
	String points;
	
	public STTrajectory() {
		// TODO Auto-generated constructor stub
		super(0,0,0,0);
	}
	
	public STTrajectory(String text) throws ParseException {
		this.fromText(new Text(text));
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(id);
		out.writeUTF(points);
		super.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id = in.readUTF();
		points = in.readUTF();
		super.readFields(in);
	}
	
	@Override
	public Text toText(Text text) {
		// TODO Auto-generated method stub
		byte[] separator = new String(";").getBytes();
		text.append(id.getBytes(), 0, id.getBytes().length);
		text.append(separator, 0,separator.length);
		text.append(points.getBytes(), 0, points.getBytes().length);
		text.append(separator, 0,separator.length);
		return super.toText(text);
	}
	
	@Override
	public void fromText(Text text) {
		// TODO Auto-generated method stub
		String[] list = text.toString().split(";");
		id = list[0];
		points = list[1];
		super.fromText(new Text(list[2]));
	}
	
	@Override
	public STTrajectory clone() {
		// TODO Auto-generated method stub
		STTrajectory c = new STTrajectory();
		c.id = this.id;
		c.points = this.points;
		super.set(x1, y1, x2, y2);
		return c;
	}
	

	
	

}
