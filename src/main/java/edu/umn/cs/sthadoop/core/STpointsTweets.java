package edu.umn.cs.sthadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * A data type used to index tweets for Taghreed project.
 * @author Louai Alarabi
 */
public class STpointsTweets extends STPoint{
	private static final Log LOG = LogFactory.getLog(STpointsTweets.class);
	  public long tweet_id;
	  public long user_id;
	  public String screen_name;
	  public String tweet_text;
	  public int follower_count;
	  public String language;
	  public String osystem;
	  
	  public STpointsTweets() {
		// TODO Auto-generated constructor stub
	}
	  
	  public STpointsTweets(String text) throws ParseException {
		  String[] list = text.toString().split(",");
		  tweet_id = Long.parseLong(list[0]);
		  user_id = Long.parseLong(list[1]);
		  screen_name = list[2];
		  tweet_text = list[3];
		  follower_count = Integer.parseInt(list[4]);
		  language = list[5];
		  osystem = list[6];
		  super.fromText(new Text(list[7]+","+list[8]+","+list[9]));

	}
	

  @Override
  public void write(DataOutput out) throws IOException {
      out.writeLong(tweet_id);
      out.writeLong(user_id);
      out.writeUTF(screen_name);
      out.writeUTF(tweet_text);
      out.writeInt(follower_count);
      out.writeUTF(language);
      out.writeUTF(osystem);
      super.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
      tweet_id = in.readLong();
      user_id = in.readLong();
      screen_name = in.readUTF();
      tweet_text = in.readUTF();
      follower_count = in.readInt();
      language = in.readUTF();
      osystem = in.readUTF();
      super.readFields(in);
    
  }

  @Override
  public Text toText(Text text) {
	byte[] separator = new String(",").getBytes();
    TextSerializerHelper.serializeLong(tweet_id, text, ',');
    TextSerializerHelper.serializeLong(user_id, text, ',');
    text.append(screen_name.getBytes(), 0, screen_name.getBytes().length);
    text.append(separator, 0,separator.length);
    text.append(tweet_text.getBytes(), 0, tweet_text.getBytes().length);
    text.append(separator, 0,separator.length);
    TextSerializerHelper.serializeInt(follower_count, text, ',');
    text.append(language.getBytes(), 0, language.getBytes().length);
    text.append(separator, 0,separator.length);
    text.append(osystem.getBytes(), 0, osystem.getBytes().length);
    text.append(separator, 0,separator.length);
    super.toText(text);
    return text;
  }

  @Override
  public void fromText(Text text) {
	  String[] list = text.toString().split(",");
	  tweet_id = Long.parseLong(list[0]);
	  user_id = Long.parseLong(list[1]);
	  screen_name = list[2];
	  tweet_text = list[3];
	  follower_count = Integer.parseInt(list[4]);
	  language = list[5];
	  osystem = list[6];
	  super.fromText(new Text(list[7]+","+list[8]+","+list[9]));
	  
  }

  @Override
  public STpointsTweets clone() {
    STpointsTweets c = new STpointsTweets();
    c.tweet_id = this.tweet_id;
    c.user_id  = this.user_id;
    c.screen_name = this.screen_name;
    c.tweet_text = this.tweet_text;
    c.follower_count = this.follower_count;
    c.language = this.language;
    c.osystem = this.osystem;
    super.set(x, y,time);
    return c;
  }
  
  public static void main(String[] args){
	  String temp = "658165748653690882,375915680,beckyfinnz,got me daytrippin' #estelle #kaskade  #roomies #waiting @ Pier 94 https://t.co/INiCDwlbjG,437,en,Instagram,"
	  		+ "2015-10-25 01:18,40.76971376,-73.99460931"; 
	  
		STpointsTweets point = new STpointsTweets();
		point.fromText(new Text(temp));
		STPoint point3d  = (STPoint) point;
		System.out.println(point.time);
		System.out.println(point3d.time);
		
		// Test casting from 3D to 2D shape.
		Point point2D = (Point) point;
		Text txt = new Text();
		point.toText(txt);
		System.out.println("Point : "+txt.toString());
		System.out.println("Point3D : "+point3d.toString());
		System.out.println("Point2D : "+point2D.toString());
	
  }

}
