package edu.umn.cs.sthadoop.core;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import edu.umn.cs.sthadoop.core.TimeFormatST.TimeFormatEnum;

public class Slice {
	private Date t1; 
	private Date t2;
	private SimpleDateFormat dateFormat;
	
	public Slice(String fromTime, String toTime, String format) throws ParseException{
		dateFormat = new SimpleDateFormat(format);
		t1 = dateFormat.parse(fromTime);
		t2 = dateFormat.parse(toTime);
	}
	
	public boolean contains(Slice timeRange){
		
		if ((timeRange.t1.compareTo(this.t1) >= 0)
				&& (timeRange.t2.compareTo(this.t2) <= 0)) {
			return true;
		}
		return false;
	}
	
	
	public static void main(String args[]) throws ParseException{
		System.out.println("Test time slice");
		String t1 = "2017-01-2";
		String t2 = "2017-01-2";
		String r1 = "2017-01-02 12:00";
		String r2 = "2017-01-08 12:00";
		TimeFormatST format1 = new TimeFormatST(TimeFormatEnum.week);
		TimeFormatST format2 = new TimeFormatST(TimeFormatEnum.day);
		Slice split = new Slice(t1, t2,format1.getSimpleDateFormat());
		Slice range = new Slice(r1, r2,format2.getSimpleDateFormat());
		if(split.contains(range))
			System.out.println("Range inside the slice");
		else
			System.out.println("Range outside the slice");
		
	}
	

}
