package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Temporal extends Point {
	private static final Log LOG = LogFactory.getLog(Temporal.class);
	public Long timeStamp;
	public double x; 
	public double y;

	public Temporal() {

	}

	public Temporal(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(timeStamp);
		out.writeDouble(timeStamp.doubleValue());
		out.writeDouble(timeStamp.doubleValue());
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.timeStamp = in.readLong();
		
	}

}
