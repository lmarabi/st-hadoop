package edu.umn.cs.spatialHadoop.core;

import java.awt.Graphics;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

public class Temporal implements StShape, Comparable<Temporal> {
	public long t;

	public Temporal() {
		this(0);
	}

	public Temporal(long t) {
		set(t);
	}

	/**
	 * A copy constructor from any shape of type Point (or subclass of Point)
	 * 
	 * @param s
	 */
	public Temporal(Temporal timeStamp) {
		this.t = timeStamp.t;
	}

	public void set(long t) {
		this.t = t;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.t = in.readLong();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(t);

	}

	@Override
	public Text toText(Text text) {
		TextSerializerHelper.serializeHexLong(t, text, '\0');
		return text;
	}

	@Override
	public void fromText(Text text){
		t = TextSerializerHelper.consumeHexLong(text, '\0');
	}
	
	@Override
	public Interval getMBR() {
		return new Interval(t,t+1);
	}

	@Override
	public long distanceTo(long t1, long t2) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isIntersected(StShape s) {
		// TODO Auto-generated method stub
		return getMBR().isIntersected(s);
	}

	@Override
	public StShape clone() {
		return new Temporal(this.t);
	}



	@Override
	public void draw(Graphics g, double xscale, double yscale) {
		// TODO Auto-generated method stub

	}

	@Override
	public int compareTo(Temporal obj) {
		return Long.compare(this.t, obj.t) ;
	}

	@Override
	public void draw(Graphics g, Interval fileMBR, int imageWidth,
			int imageHeight, double scale) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(StShape s) {
		Temporal timeStamp = (Temporal)s;
		if(this.t < timeStamp.t)
			return-1;
		if(this.t == timeStamp.t)
			return 0;
		return 1;
	}
	
}
