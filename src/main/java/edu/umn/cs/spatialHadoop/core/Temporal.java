package edu.umn.cs.spatialHadoop.core;

import java.awt.Graphics;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

public class Temporal implements Shape, Comparable<Temporal> {
    public Long timeStamp;
	public double x;
	public double y;

	public Temporal() {
		this(0);
	}
	
	public Temporal(long t) {
	  set(t);
	}
	

	/**
	 * A copy constructor from any shape of type Point (or subclass of Point)
	 * @param s
	 */
	public Temporal(Temporal s) {
	  this.timeStamp = s.timeStamp;
	  this.x = this.timeStamp.doubleValue();
	  this.y = this.timeStamp.doubleValue();
  }

  public void set(long t) {
	    this.timeStamp = t;
		this.x = this.timeStamp.doubleValue();
		this.y = this.timeStamp.doubleValue();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(timeStamp);
	}

	public void readFields(DataInput in) throws IOException {
		this.timeStamp = in.readLong();
		this.x = this.timeStamp.doubleValue();
		this.y = this.timeStamp.doubleValue();
	}

	public int compareTo(Shape s) {
	  Temporal pt2 = (Temporal) s;

	  // Sort by id
	  double difference = this.x - pt2.x;
		if (difference == 0) {
			difference = this.y - pt2.y;
		}
		if (difference == 0)
		  return 0;
		return difference > 0 ? 1 : -1;
	}
	
	public boolean equals(Object obj) {
		if (obj == null) 
			return false;
		Temporal r2 = (Temporal) obj;
		return this.x == r2.x && this.y == r2.y;
	}

	@Override
	public int hashCode() {
		int result;
		long temp;
		temp = Double.doubleToLongBits(this.x);
		result = (int) (temp ^ temp >>> 32);
		temp = Double.doubleToLongBits(this.y);
		result = 31 * result + (int) (temp ^ temp >>> 32);
		return result;
	}

	public double distanceTo(Temporal s) {
		double dx = s.x - this.x;
		double dy = s.y - this.y;
		return Math.sqrt(dx*dx+dy*dy);
	}
	
	@Override
	public Temporal clone() {
	  return new Temporal(this.timeStamp);
	}

	/**
	 * Returns the minimal bounding rectangle of this point. This method returns
	 * the smallest rectangle that contains this point. For consistency with
	 * other methods such as {@link Rectangle#isIntersected(Shape)}, the rectangle
	 * cannot have a zero width or height. Thus, we use the method
	 * {@link Math#ulp(double)} to compute the smallest non-zero rectangle that
	 * contains this point. In other words, for a point <code>p</code> the
	 * following statement should return true.
	 * <code>p.getMBR().isIntersected(p);</code>
	 */
  @Override
  public Rectangle getMBR() {
    return new Rectangle(x, y, x + Math.ulp(x), y + Math.ulp(y));
  }

  @Override
  public double distanceTo(double px, double py) {
    double dx = x - px;
    double dy = y - py;
    return Math.sqrt(dx * dx + dy * dy);
  }

  public Shape getIntersection(Shape s) {
    return getMBR().getIntersection(s);
  }

  @Override
  public boolean isIntersected(Shape s) {
    return getMBR().isIntersected(s);
  }
  
  @Override
  public String toString() {
    return "Temporal: ("+timeStamp+", "+x+", "+y+")";
  }
  
  @Override
  public Text toText(Text text) {
	TextSerializerHelper.serializeLong(timeStamp, text, '\0');
    return text;
  }
  
  @Override
  public void fromText(Text text) {
    timeStamp = TextSerializerHelper.consumeHexLong(text, '\0');
    x = timeStamp.doubleValue();
    y = timeStamp.doubleValue();
  }

  @Override
  public int compareTo(Temporal o) {
    if (x < o.x)
      return -1;
    if (x > o.x)
      return 1;
    if (y < o.y)
      return -1;
    if (y > o.y)
      return 1;
    return 0;
  }

  @Override
  public void draw(Graphics g, Rectangle fileMBR, int imageWidth,
  		int imageHeight, double scale) {
    int imageX = (int) Math.round((this.x - fileMBR.x1) * imageWidth / fileMBR.getWidth());
    int imageY = (int) Math.round((this.y - fileMBR.y1) * imageHeight / fileMBR.getHeight());
    g.fillRect(imageX, imageY, 1, 1);  	
  }
  
  @Override
  public void draw(Graphics g, double xscale, double yscale) {
    int imgx = (int) Math.round(x * xscale);
    int imgy = (int) Math.round(y * yscale);
    g.fillRect(imgx, imgy, 1, 1);
  }

}
