package edu.umn.cs.sthadoop.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.sthadoop.core.STPoint;

/**
 *  
 * @author louai Alarabi
 *
 */

public class HdfsDataPartitions<S extends STPoint> implements Writable {
	//public List<S> qSet;
	public List<S> refSet;
	
	public HdfsDataPartitions() {
		//qSet = new Vector<S>();
		refSet = new Vector<S>();
	}
	
	public HdfsDataPartitions(int querySize, int refSize) {
		//qSet = new Vector<S>(querySize);
		refSet = new Vector<S>(refSize);
	}
	
	public HdfsDataPartitions(List<S> inQuery, List<S> inRef) {
		//this.qSet = inQuery;
		this.refSet = inRef;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
//		for (S shape : qSet) {
//			shape.readFields(in);
//		}
		for (S shape : refSet) {
			shape.readFields(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
//		for (S shape : qSet) {
//			shape.write(out);
//		}
		for (S shape : refSet) {
			shape.write(out);
		}
	}
}
