/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umn.cs.sthadoop.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.sthadoop.core.TimeFormatST.TimeFormatEnum;

/**
 *
 * @author louai
 */
public class Lookup {
	private Path indexPath;
	private FileSystem fileSystem;
	private TimeFormatST timeFormat;

	// private List<String> yearDates = new ArrayList<String>();
	private List<Path> yearPaths = new ArrayList<Path>();

	// private List<String> dayDates = new ArrayList<String>();
	private List<Path> dayPaths = new ArrayList<Path>();

	// private List<String> monthDates = new ArrayList<String>();
	private List<Path> monthPaths = new ArrayList<Path>();

	// private List<Week> weekDates = new ArrayList<Week>();
	private List<Path> weekPaths = new ArrayList<Path>();

	public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private String dirPath;

	/**
	 * Constructor and load the Spatiotemporal Index direcotry.
	 * 
	 * @param params
	 * @throws Exception
	 */
	public Lookup(OperationsParams params) throws Exception {
		indexPath = params.getInputPath();
		this.fileSystem = this.indexPath.getFileSystem(new Configuration());
		// load index lower resolution layers to high resolution layer.
		this.dayPaths = getlistSlice(TimeFormatEnum.day);
		this.yearPaths = getlistSlice(TimeFormatEnum.year);
		this.monthPaths = getlistSlice(TimeFormatEnum.month);
		this.weekPaths = getlistSlice(TimeFormatEnum.week);

	}

	/**
	 * This method get all list of indexed slices from indexPath directory based
	 * on the level.
	 * 
	 * @param resolution
	 * @return
	 * @throws Exception
	 * @throws IOException
	 */
	private List<Path> getlistSlice(TimeFormatEnum resolution) throws Exception, IOException {
		ArrayList<Path> result = new ArrayList<Path>();
		this.timeFormat = new TimeFormatST(resolution);
		Path parentindex = new Path(this.indexPath.toString() + "/" + this.timeFormat.getSimpleDateFormat());
		if (this.fileSystem.exists(parentindex)) {
			FileStatus[] indexesFiles = fileSystem.listStatus(parentindex);
			for (FileStatus index : indexesFiles) {
				if (index.isDirectory()) {
					result.add(index.getPath());
				}
			}
		}else{
			System.out.println(parentindex.getName()+"  doesn't exist");
		}
		return result;
	}

	/**
	 * This method return paths of all dates between the start and the end date.
	 *
	 * @param startDate
	 * @param endDate
	 * @return
	 * @throws ParseException
	 */
	public List<Path> getTweetsDayIndex(String startDate, String endDate) throws ParseException {
		ArrayList<Path> result = new ArrayList<Path>();
		for (int i = 0; i < dayPaths.size(); i++) {
			if (insideDaysBoundry(startDate, endDate, dayPaths.get(i).getName())) {
				result.add(dayPaths.get(i));
			}
		}
		return result;
	}
	
	
	public List<String> getDay(String startDate, String endDate) throws ParseException{
		Date start = dateFormat.parse(startDate);
		Date end = dateFormat.parse(endDate);
		Calendar cstart = Calendar.getInstance();
		Calendar cend = Calendar.getInstance();
		cstart.setTime(start);
		cend.setTime(end);
		List<String> intermediateResult = new ArrayList<String>();
		while (!(cstart.get(Calendar.YEAR) == cend.get(Calendar.YEAR)
				&& (cstart.get(Calendar.MONTH) == cend.get(Calendar.MONTH))
				&& (cstart.get(Calendar.DATE) == cend.get(Calendar.DATE)))) {
			String day = (cstart.get(Calendar.DAY_OF_MONTH) >= 10) ? String.valueOf(cstart.get(Calendar.DAY_OF_MONTH))
					: "0" + String.valueOf(cstart.get(Calendar.DAY_OF_MONTH));
			String month = ((cstart.get(Calendar.MONTH) + 1) >= 10) ? String.valueOf((cstart.get(Calendar.MONTH) + 1))
					: "0" + String.valueOf((cstart.get(Calendar.MONTH) + 1));
			String date = cstart.get(Calendar.YEAR) + "-" + month + "-" + day;
			if (!intermediateResult.contains(date)) {
				intermediateResult.add(date);
			}
			cstart.add(Calendar.DATE, 1);

		}
		return intermediateResult;
	}

	/**
	 * This method return HashMap<Date,Path> to all dates between the start and
	 * the end date.
	 *
	 * @param startDate
	 * @param endDate
	 * @return
	 * @throws ParseException
	 */
	public List<String> getWeek(String startDate, String endDate) throws ParseException {
		Date start = dateFormat.parse(startDate);
		Date end = dateFormat.parse(endDate);
		Calendar cstart = Calendar.getInstance();
		Calendar cend = Calendar.getInstance();
		cstart.setTime(start);
		cend.setTime(end);
		List<String> intermediateResult = new ArrayList<String>();
		while (!(cstart.get(Calendar.YEAR) == cend.get(Calendar.YEAR)
				&& (cstart.get(Calendar.MONTH) == cend.get(Calendar.MONTH))
				&& (cstart.get(Calendar.DATE) == cend.get(Calendar.DATE)))) {
			String month = ((cstart.get(Calendar.MONTH) + 1) >= 10) ? String.valueOf((cstart.get(Calendar.MONTH) + 1))
					: "0" + String.valueOf((cstart.get(Calendar.MONTH) + 1));
			String weekofDay = cstart.get(Calendar.YEAR) + "-" + month + "-"
					+ cstart.get(Calendar.WEEK_OF_MONTH);
			if (!intermediateResult.contains(weekofDay)) {
				intermediateResult.add(weekofDay);
			}
			cstart.add(Calendar.DATE, 1);

		}
		return intermediateResult;
	}

	public List<String> getMonth(String startDate, String endDate) throws ParseException {
		List<String> result = new ArrayList<String>();
		Date start = dateFormat.parse(startDate);
		Date end = dateFormat.parse(endDate);
		Calendar c = Calendar.getInstance();
		c.setTime(start);
		String month;
		while (start.getYear() != end.getYear() || start.getMonth() != end.getMonth()) {
			month = ((start.getMonth() + 1) >= 10) ? String.valueOf(start.getMonth() + 1)
					: "0" + String.valueOf(start.getMonth() + 1);
			result.add(c.get(Calendar.YEAR) + "-" + month);
			c.add(Calendar.MONTH, 1);
			start = c.getTime();

		}
		month = ((start.getMonth() + 1) >= 10) ? String.valueOf(start.getMonth() + 1)
				: "0" + String.valueOf(start.getMonth() + 1);
		result.add(c.get(Calendar.YEAR) + "-" + month);
		return result;
	}





	/**
	 * This method return true if lookupdate within start,end time window
	 *
	 * @param start
	 * @param end
	 * @param lookupDate
	 * @return
	 * @throws ParseException
	 */
	public static boolean insideDaysBoundry(String start, String end, String lookupStringDate) throws ParseException {
		Date startDate = dateFormat.parse(start);
		Date endDate = dateFormat.parse(end);
		Date lookupDate = dateFormat.parse(lookupStringDate);
		if ((lookupDate.compareTo(startDate) >= 0) && (lookupDate.compareTo(endDate) <= 0)) {
			return true;
		}
		return false;
	}

	// public static boolean insideWeekBoundry(String start, String end, Week
	// range)
	// throws ParseException {
	// if (insideDaysBoundry(start, end, range.getStart())
	// && insideDaysBoundry(start, end, range.getEnd())) {
	// return true;
	// }
	// return false;
	// }

	public static void main(String[] args) throws Exception {
		args = new String[3];
		args[0] = "/home/louai/nyc-taxi/result/";
		args[1] = "shape:edu.umn.cs.sthadoop.core.STPoint";
		// "shape:edu.umn.cs.sthadoop.core.STpointsTweets";
		args[2] = "time:month";
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args), false);
		Lookup l = new Lookup(params);
		List<String> result = l.getMonth("2015-01-01", "2015-12-01");
		System.out.println("Result month: \n");
		for(String x : result)
			System.out.println(x);
		
		
		result = l.getWeek("2015-01-01", "2015-12-01");
		System.out.println("Result week: \n");
		for(String x : result)
			System.out.println(x);
		
		result = l.getDay("2015-01-01", "2015-02-03");
		System.out.println("Result Day: \n");
		for(String x : result)
			System.out.println(x);

	}
}
