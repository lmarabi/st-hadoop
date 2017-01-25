/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umn.cs.sthadoop.core;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.sun.org.apache.xerces.internal.impl.dv.xs.DayDV;

/**
 *
 * @author turtle
 */
public class Week {
//    private Date start;
//    private Date end;
    private String weekName;
    private int year; 
    private int month;
    private int weekNumber;
    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public Week(String weekName) {
    	this.weekName = weekName;
    	String[] temp = weekName.split("-");
    	this.year = Integer.parseInt(temp[0]);
    	this.month = Integer.parseInt(temp[1]);
    	this.weekNumber = Integer.parseInt(temp[2]);
    }

//    public Week(Date start, Date end) {
//        this.start = start;
//        this.end = end;
//    }
//    
//    public String getStart() {
//        return dateFormat.format(this.start);
//    }
//
//    public void setStart(Date start) {
//        this.start = start;
//    }

//    public String getEnd() {
//        return dateFormat.format(this.end);
//    }
//
//    public void setEnd(Date end) {
//        this.end = end;
//    }
    
    public String getWeekName() {
		return weekName;
	}
    
    /**
     * This method parse the expression 2013-01-15&2014-10-12 To week object
     * @param args
     * @return
     * @throws ParseException 
     */
    public Week parseToWeek(String args) throws ParseException{
//        String[] temp = args.split(",");
//        String[] range = temp[0].split("&");
        return new Week(args);
    }
    
    
    
    /**
     * This method check whether the day in a week or not
     * @param day
     * @return 
     * @throws ParseException 
     */
     public boolean isDayIntheWeek(String date) throws ParseException {
    	 Calendar c = Calendar.getInstance();
         Date day = dateFormat.parse(date);
         c.setTime(day);
          if(this.month == c.get(Calendar.MONTH) && this.weekNumber ==
                  c.get(Calendar.WEEK_OF_MONTH) &&
                  this.year == c.get(Calendar.YEAR)){
              return true;
          }
         return false;
     }

   
    
    @Override
    public String toString() {
    	return this.weekName;
    }
    
    
    
}
