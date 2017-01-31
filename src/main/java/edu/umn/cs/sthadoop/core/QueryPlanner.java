/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umn.cs.sthadoop.core;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

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
public class QueryPlanner {
	private Path indexPath;
	private FileSystem fileSystem;
	private TimeFormatST timeFormat;

	private List<Path> yearPaths;
	private List<Path> dayPaths;
	private List<Path> monthPaths;
	private List<Path> weekPaths;
	private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

	/**
	 * Constructor and load the Spatiotemporal Index directory.
	 * 
	 * @param params
	 * @throws Exception
	 */
	public QueryPlanner(OperationsParams params) throws Exception {
		indexPath = params.getInputPath();
		this.fileSystem = this.indexPath.getFileSystem(new Configuration());
		// load index lower resolution layers to high resolution layer.
		this.dayPaths = getlistSlice(TimeFormatEnum.day);
		this.yearPaths = getlistSlice(TimeFormatEnum.year);
		this.monthPaths = getlistSlice(TimeFormatEnum.month);
		this.weekPaths = getlistSlice(TimeFormatEnum.week);

	}
	
	/**
	 * This method return list of temporal slices that should be queried 
	 * @param time1
	 * @param time2
	 * @return List<Path> , each path represent indexed spatio-temporal slice. 
	 * @throws ParseException
	 */
	public List<Path> getQueryPlan(String time1, String time2) throws ParseException{
		List<String> planY = this.getYear(time1, time2);
		List<String> planM = this.getMonth(time1, time2);
		List<String> planW = this.getWeek(time1, time2);
		List<String> planD = this.getDay(time1, time2);
		List<Path> result = new ArrayList<Path>();
		int min =0; 
		TimeFormatEnum resolution = null; 
		if(this.dayPaths.size() > 0){
			min = planD.size();
			resolution = TimeFormatEnum.day;
			if(this.weekPaths.size() > 0 && min > this.weekPaths.size()){
				min = planW.size(); 
				resolution = TimeFormatEnum.week;
				if(this.monthPaths.size() > 0 && min > this.monthPaths.size()){
					min = planM.size();
					resolution = TimeFormatEnum.month;
					if(this.yearPaths.size() > 1 && min > this.yearPaths.size()){
						min = planY.size();
						resolution = TimeFormatEnum.year;
					}
				}
			}
		}
		this.timeFormat = new  TimeFormatST(resolution);
		List<String> queryplan = null;
		switch(resolution){
		case day:
			queryplan = planD;
			break;
		case week:
			queryplan = planW;
			break;
		case month:
			queryplan = planM;
			break; 
		case year:
			queryplan = planY;
			break;
		default:
			queryplan = planD;
			break;
			
		}
		for(String dir : queryplan){
			result.add(new Path(this.indexPath.toString() + "/" + this.timeFormat.getSimpleDateFormat()+"/"+ dir));
		}
		return result;
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
	 * This method return all days between time interval passed as parameter. 
	 * @param startDate
	 * @param endDate
	 * @return List<String> each in a format of yyyy-MM-dd 
	 * @throws ParseException
	 */
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
		
		// add the last day 
		String day = (cstart.get(Calendar.DAY_OF_MONTH) >= 10) ? String.valueOf(cstart.get(Calendar.DAY_OF_MONTH))
				: "0" + String.valueOf(cstart.get(Calendar.DAY_OF_MONTH));
		String month = ((cstart.get(Calendar.MONTH) + 1) >= 10) ? String.valueOf((cstart.get(Calendar.MONTH) + 1))
				: "0" + String.valueOf((cstart.get(Calendar.MONTH) + 1));
		String date = cstart.get(Calendar.YEAR) + "-" + month + "-" + day;
		if (!intermediateResult.contains(date)) {
			intermediateResult.add(date);
		}
		
		return intermediateResult;
	}

	/**
	 * This method return all weeks between the start and
	 * the end date that represent time interval. 
	 *
	 * @param startDate
	 * @param endDate
	 * @return List<String> each in a format of yyyy-MM-w
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

	/***
	 * This method return all months contains the time interval passed in the parameters. 
	 * @param startDate
	 * @param endDate
	 * @return List<String> each in a format of yyyy-MM
	 * @throws ParseException
	 */
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
	
	
	/***
	 * This method return all years contains the time interval passed in the parameters. 
	 * @param startDate
	 * @param endDate
	 * @return List<String> each in a format of yyyy
	 * @throws ParseException
	 */
	public List<String> getYear(String startDate, String endDate) throws ParseException {
		List<String> result = new ArrayList<String>();
		Date start = dateFormat.parse(startDate);
		Date end = dateFormat.parse(endDate);
		Calendar c = Calendar.getInstance();
		c.setTime(start);
		String month;
		while (start.getYear() <= end.getYear()) {
			result.add(String.valueOf(c.get(Calendar.YEAR)));
			c.add(Calendar.YEAR, 1);
			start = c.getTime();

		}
		return result;
	}

	
	public static void main(String[] args) throws Exception {
		args = new String[6];
		args[0] = "/home/louai/nyc-taxi/yellowIndex";
		args[1] = "/home/louai/nyc-taxi/resultSTRQ";
		args[2] = "shape:edu.umn.cs.sthadoop.core.STPoint";
		args[3] = "rect:-74.98451232910156,35.04014587402344,-73.97936248779295,41.49399566650391";
		args[4] = "interval:2015-01-01,2015-01-02";
		args[5] = "-overwrite";
		final OperationsParams params = new OperationsParams(new GenericOptionsParser(args), false);
		QueryPlanner l = new QueryPlanner(params);
		List<Path> result = l.getQueryPlan("2015-01-01", "2015-12-01");
		System.out.println("Result: \n");
		for(Path x : result)
			System.out.println(x.toString());
		
//		
//		result = l.getWeek("2015-01-01", "2015-12-01");
//		System.out.println("Result week: \n");
//		for(String x : result)
//			System.out.println(x);
		
//		result = l.getDay("2015-01-01", "2015-02-03");
//		System.out.println("Result Day: \n");
//		for(String x : result)
//			System.out.println(x);
		
		

//		result = l.getYear("2015-01-01", "2020-02-03");
//		System.out.println("Result year: \n");
//		for(String x : result)
//			System.out.println(x);

	}
}
