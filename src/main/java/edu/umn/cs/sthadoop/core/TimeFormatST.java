package edu.umn.cs.sthadoop.core;


/**
 * This class used to unified the naming of the folders and also used for SimpleDateFormat for the data. 
 * @author louai
 *
 */
public class TimeFormatST {
	
	private TimeFormatEnum time; 
	
	public static enum TimeFormatEnum {
		year, month, week, day, minute;
	}
	
	public TimeFormatST(TimeFormatEnum format) {
		this.time = format;
	}
	
	
	
	public String getSimpleDateFormat(){
		String spaceFormat = ""; 
		switch (this.time) {
		case minute:
			spaceFormat = "yyyy-MM-dd HH:mm";
			break;
		case day:
			spaceFormat = "yyyy-MM-dd";
			break;
		case week:
			spaceFormat = "yyyy-MM-W";
			break;
		case month:
			spaceFormat = "yyyy-MM";
			break;
		case year:
			spaceFormat = "yyyy";
			break;
		default:
			spaceFormat = "yyyy-MM-dd";
			break;
		}
		return spaceFormat;
	}

}
