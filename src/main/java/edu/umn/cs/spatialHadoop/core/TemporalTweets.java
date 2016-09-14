package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

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
public class TemporalTweets extends Point {
	private static final Log LOG = LogFactory.getLog(TemporalTweets.class);
	public Long created_at;
	public double x;
	public double y;

	public TemporalTweets() {
		// TODO Auto-generated constructor stub
	}

	public TemporalTweets(String text) throws ParseException {
		String[] list = text.toString().split(",");
		if (list.length == 3) {
			try {
				created_at = Long.parseLong(list[0]);
			} catch (NumberFormatException exception) {
				created_at = (long) 0;
				exception.printStackTrace();
			}
			x = created_at.doubleValue();
			y = created_at.doubleValue();
			super.fromText(new Text(x + "," + y));
		}
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
		TextSerializerHelper.serializeLong(created_at, text, ',');
		super.toText(text);
		return text;
	}

	@Override
	public void fromText(Text text) {
		String[] list = text.toString().split(",");
		if (list.length == 3) {
			try {
				created_at = Long.parseLong(list[0]);
			} catch (NumberFormatException exception) {
				created_at = (long) 0;
				exception.printStackTrace();
			}
			x = created_at.doubleValue();
			y = created_at.doubleValue();
			super.fromText(new Text(x + "," + y));
		}
	}

	@Override
	public TemporalTweets clone() {
		TemporalTweets c = new TemporalTweets();
		c.created_at = this.created_at;
		super.set(x, y);
		return c;
	}

}
