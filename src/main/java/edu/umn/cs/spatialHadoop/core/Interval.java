package edu.umn.cs.spatialHadoop.core;

import java.awt.Graphics;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

public class Interval implements StShape, WritableComparable<Interval> {
	public long t1;
	public long t2;

	public Interval() {
		this(0, 0);
	}

	public Interval(Interval r) {
		this(r.t1, r.t2);
	}

	public Interval(long t1, long t2) {
		this.set(t1, t2);
	}

	public void set(StShape s) {
		if (s == null) {
			System.out.println("tozz");
			return;
		}
		Interval mbr = s.getMBR();
		set(mbr.t1, mbr.t2);
	}

	public void set(long t1, long t2) {
		this.t1 = t1;
		this.t2 = t2;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(t1);
		out.writeLong(t2);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.t1 = in.readLong();
		this.t2 = in.readLong();
	}

	@Override
	public int compareTo(Interval rect) {
		if (this.t1 < rect.t1)
			return -1;
		if (this.t1 > rect.t1)
			return 1;
		if (this.t2 < rect.t2)
			return -1;
		if (this.t2 > rect.t2)
			return 1;
		return 0;
	}

	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		Interval rect = (Interval) obj;
		boolean result = this.t1 == rect.t1 && this.t2 == rect.t2;
		return result;
	}

	@Override
	public int hashCode() {
		int result;
		long temp;
		temp = Double.doubleToLongBits(this.t1);
		result = (int) (temp ^ temp >>> 32);
		temp = Double.doubleToLongBits(this.t2);
		result = 31 * result + (int) (temp ^ temp >>> 32);
		return result;
	}

	@Override
	public long distanceTo(long t1, long t2) {
		return getMaxDistanceTo(t1, t2);
	}

	/**
	 * Maximum distance to the perimeter of the Rectangle
	 * 
	 * @param px
	 * @param py
	 * @return
	 */
	public long getMaxDistanceTo(long pt1, long pt2) {
		Interval rect = new Interval(pt1, pt2);
		if (this.equals(rect))
			return 0;
		if (this.isIntersected(rect))
			return 0;
		long maxt1 = this.t2 - pt1;
		long maxt2 = this.t1 - pt2;
		return Math.max(maxt1, maxt2);
	}

	public double getMinDistanceTo(long pt1, long pt2) {
		Interval rect = new Interval(pt1, pt2);
		if (this.equals(rect))
			return 0;
		if (this.isIntersected(rect))
			return 0;
		long mint1 = this.t2 - pt1;
		long mint2 = this.t1 - pt2;
		return Math.min(mint1, mint2);
	}

	@Override
	public Interval clone() {
		return new Interval(this);
	}

	@Override
	public boolean isIntersected(StShape s) {
		if (s instanceof Temporal) {
			Temporal timeStamp = (Temporal) s;
			return timeStamp.t <= this.t2 && timeStamp.t >= this.t1;
		}
		Interval rect = s.getMBR();
		if (rect == null)
			return false;
		return rect.t1 < this.t2 && this.t1 < rect.t2;
	}

	public Interval getIntersection(StShape s) {
		if (!s.isIntersected(this))
			return null;
		Interval rect = s.getMBR();
		if (this.equals(rect))
			return rect;
		if (this.getWidth() > rect.getWidth()) {
			return this;
		}
		return rect;
	}

	public boolean contains(Temporal t) {
		return isIntersected(t);
	}
	
	public boolean contains(long t1, long t2){
		return t1 < this.t2 && this.t1 < t2;
	}
	
	public Temporal getCenterPoint() {
		return new Temporal((long)t2/2);
	}

	public Interval union(final StShape s) {
		Interval r = s.getMBR();
		Long min = Math.min(t1, t2);
		Long max = Math.max(r.t1, r.t2);
		return new Interval(min, max);
	}

	public void expand(final StShape s) {
		Interval r = s.getMBR();
		if (r.t1 < this.t1)
			this.t1 = r.t1;
		if (r.t2 > this.t2)
			this.t2 = r.t2;

	}

	public long getWidth() {
		return t2 - t1;
	}

	@Override
	public Text toText(Text text) {
		TextSerializerHelper.serializeLong(t1, text, ',');
		TextSerializerHelper.serializeLong(t2, text, '\0');
		return text;
	}

	@Override
	public void fromText(Text text) {
		t1 = TextSerializerHelper.consumeHexLong(text, ',');
		t2 = TextSerializerHelper.consumeHexLong(text, '\0');
	}

	@Override
	public String toString() {
		return "Interval: (" + t1 + "," + t2 + ")";
	}
	
	 public boolean isValid() {
		    return true;
		  }
		  

	@Override
	public Interval getMBR() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void draw(Graphics g, Interval fileMBR, int imageWidth,
			int imageHeight, double scale) {
		// TODO Auto-generated method stub

	}

	@Override
	public void draw(Graphics g, double xscale, double yscale) {
		// TODO Auto-generated method stub

	}

	@Override
	public int compareTo(StShape s) {
		Interval rect = (Interval) s.getMBR();
		if (this.t1 < rect.t1)
			return -1;
		if (this.t1 > rect.t1)
			return 1;
		if (this.t2 < rect.t2)
			return -1;
		if (this.t2 > rect.t2)
			return 1;
		return 0;
	}

}
