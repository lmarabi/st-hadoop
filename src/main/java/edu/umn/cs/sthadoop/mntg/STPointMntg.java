package edu.umn.cs.sthadoop.mntg;

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
 * A data type used trajectories dataset from MNTG.
 * @author Louai Alarabi
 */
public class STPointMntg extends STPoint{
	private static final Log LOG = LogFactory.getLog(STPointMntg.class);
	  public String id;
	  public String type;
	  
	  public STPointMntg() {
		// TODO Auto-generated constructor stub
	}
	  
	  public STPointMntg(String text) throws ParseException {
		  String[] list = text.toString().split(",");
		  id = list[0];
		  type = list[1];
		  super.fromText(new Text(list[2]+","+list[3]+","+list[4]));

	}
	

  @Override
  public void write(DataOutput out) throws IOException {
      out.writeUTF(id);
      out.writeUTF(type);
      super.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
      id = in.readUTF();
      type = in.readUTF();
      super.readFields(in);
    
  }

  @Override
  public Text toText(Text text) {
	byte[] separator = new String(",").getBytes();
    text.append(id.getBytes(), 0, id.getBytes().length);
    text.append(separator, 0,separator.length);
    text.append(type.getBytes(), 0, type.getBytes().length);
    text.append(separator, 0,separator.length);
    super.toText(text);
    return text;
  }

  @Override
  public void fromText(Text text) {
	  String[] list = text.toString().split(",");
	  id = list[0];
	  type = list[1];
	  super.fromText(new Text(list[2]+","+list[3]+","+list[4]));
	  
  }

  @Override
  public STPointMntg clone() {
    STPointMntg c = new STPointMntg();
    c.id = this.id;
    c.type = this.type;
    super.set(x, y,time);
    return c;
  }
  
  
  @Override
  public String toString() {
		return "STPointMntg: ("+id+","+type+","+time+","+x+","+y+")";
	}
  
  public static void main(String[] args){
	  String temp = "4953-9,newpoint,2017-08-03 22:45,126.6613993,35.3278531"; 
	  
		STPointMntg point = new STPointMntg();
		point.fromText(new Text(temp));
		STPoint point3d  = (STPoint) point;
		System.out.println(point.time);
		System.out.println(point3d.time);
		
		// Test casting from 3D to 2D shape.
		Point point2D = (Point) point;
		Text txt = new Text();
		point.toText(txt);
		System.out.println("Point : "+point.toString());
		System.out.println("Point3D : "+point3d.toString());
		System.out.println("Point2D : "+point2D.toString());
	
  }

}
