package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * A data type used to index tweets for Taghreed project.
 * 
 * @author Louai Alarabi
 */
public class STPoint extends Point {
	private static final Log LOG = LogFactory.getLog(STPoint.class);
	public long tweet_id;

	public STPoint() {
		// TODO Auto-generated constructor stub
	}

	public STPoint(String text) {
		String[] list = text.toString().split(",");
		tweet_id = Long.parseLong(list[0]);
		super.fromText(new Text(list[2] + "," + list[1]));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(tweet_id);
		super.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tweet_id = in.readLong();
		super.readFields(in);

	}

	@Override
	public Text toText(Text text) {
		TextSerializerHelper.serializeLong(tweet_id, text, ',');
		super.toText(text);
		return text;
	}

	@Override
	public void fromText(Text text) {
		tweet_id = TextSerializerHelper.consumeLong(text, ',');
		super.fromText(text);

	}

	@Override
	public STPoint clone() {
		STPoint c = new STPoint();
		c.tweet_id = this.tweet_id;
		super.set(x, y);
		return c;
	}

}
