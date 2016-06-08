package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;

/**
 * A data type used to index tweets for Taghreed project.
 * 
 * @author Louai Alarabi
 */
public class SpatialTweets extends Point {
	private static final Log LOG = LogFactory.getLog(SpatialTweets.class);
	public Long created_at;

	public SpatialTweets() {
		// TODO Auto-generated constructor stub
	}

	public SpatialTweets(String text) {
		String[] list = text.toString().split(",");
		created_at = Long.parseLong(list[0]);
		super.fromText(new Text(list[1] + "," + list[2]));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(created_at);
		super.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		created_at = in.readLong();
		super.readFields(in);

	}

	@Override
	public Text toText(Text text) {
		TextSerializerHelper.serializeDouble(created_at, text, ',');
		super.toText(text);
		return text;
	}

	@Override
	public void fromText(Text text) {
		String[] list = text.toString().split(",");
		System.out.println("\n>>>>>>>>>> " + text.toString());
		created_at = Long.parseLong(list[0]);
		super.fromText(new Text(list[1] + "," + list[2]));
	}

	@Override
	public SpatialTweets clone() {
		SpatialTweets c = new SpatialTweets();
		c.created_at = this.created_at;
		super.set(x, y);
		return c;
	}

}
