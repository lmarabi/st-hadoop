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
 * @author louai Alarabi
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

	// Temporary queryPlan of MonthContained.
	private List<String> yearContained;
	private List<String> monthContained;
	private List<String> weekContained;
	private List<String> dayContained;

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
	 * 
	 * @param time1
	 * @param time2
	 * @return List<Path> , each path represent indexed spatio-temporal slice if
	 *         exist only.
	 * @throws ParseException
	 */
	public List<Path> getQueryPlan(String time1, String time2) throws ParseException {
		List<String> planY = this.getYearContainedBy(time1, time2);
		List<String> planM = this.getMonthsContainedBy(time1, time2);
		List<String> planW = this.getWeekContainedBy(time1, time2);
		List<String> planD = this.getDayContainedBy(time1, time2);
		List<Path> result = new ArrayList<Path>();

		for (TimeFormatEnum resolution : TimeFormatEnum.values()) {
			List<String> queryplan = new ArrayList<String>();
			this.timeFormat = new TimeFormatST(resolution);
			switch (resolution) {
			case day:
				queryplan = planD;
				this.timeFormat = new TimeFormatST(resolution);
				break;
			case week:
				queryplan = planW;
				this.timeFormat = new TimeFormatST(resolution);
				break;
			case month:
				queryplan = planM;
				this.timeFormat = new TimeFormatST(resolution);
				break;
			case year:
				queryplan = planY;
				this.timeFormat = new TimeFormatST(resolution);
				break;
			default:
				// do nothing This to support a higher resolution level.
				break;

			}
			for (String dir : queryplan) {
				Path sliceIndex = new Path(
						this.indexPath.toString() + "/" + this.timeFormat.getSimpleDateFormat() + "/" + dir);
				try {
					if (fileSystem.exists(sliceIndex)) {
						result.add(sliceIndex);
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return result;
	}

	/**
	 * This method return list of temporal slices that should be queried from
	 * specific level " resolution"
	 * 
	 * @param time1
	 * @param time2
	 * @return List<Path> , each path represent indexed spatio-temporal slice if
	 *         exist only.
	 * @throws ParseException
	 */
	public List<Path> getQueryPlanFromResolution(String time1, String time2, String level) throws ParseException {
		List<Path> result = new ArrayList<Path>();

		TimeFormatEnum resolution = TimeFormatEnum.valueOf(level);

		List<String> queryplan = new ArrayList<String>();
		this.timeFormat = new TimeFormatST(resolution);
		switch (resolution) {
		case day:
			queryplan = this.getDay(time1, time2);
			break;
		case week:
			queryplan = this.getWeek(time1, time2);
			break;
		case month:
			queryplan = this.getMonth(time1, time2);
			break;
		case year:
			queryplan = this.getYear(time1, time2);
			break;
		default:
			queryplan = this.getYear(time1, time2);
			break;

		}
		for (String dir : queryplan) {
			Path sliceIndex = new Path(
					this.indexPath.toString() + "/" + this.timeFormat.getSimpleDateFormat() + "/" + dir);
			try {
				if (fileSystem.exists(sliceIndex)) {
					result.add(sliceIndex);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
		} else {
			System.out.println(parentindex.getName() + "  doesn't exist");
		}
		return result;
	}

	/**
	 * This method return all days between time interval passed as parameter.
	 * 
	 * @param startDate
	 * @param endDate
	 * @return List<String> each in a format of yyyy-MM-dd
	 * @throws ParseException
	 */
	public List<String> getDay(String startDate, String endDate) throws ParseException {
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
	 * This method return all days that contained between time interval passed
	 * as parameter. This method should be called after getYearContainedBy,
	 * getMonthContainedBy, getWeekContainedBy
	 * 
	 * @param startDate
	 * @param endDate
	 * @return List<String> each in a format of yyyy-MM-dd
	 * @throws ParseException
	 */
	public List<String> getDayContainedBy(String startDate, String endDate) throws ParseException {
		Date start = dateFormat.parse(startDate);
		Date end = dateFormat.parse(endDate);
		Calendar cstart = Calendar.getInstance();
		Calendar cend = Calendar.getInstance();
		cstart.setTime(start);
		cend.setTime(end);
		List<String> intermediateResult = new ArrayList<String>();
		while (cstart.before(cend) || cstart.equals(cend)) {
			String day = (cstart.get(Calendar.DAY_OF_MONTH) >= 10) ? String.valueOf(cstart.get(Calendar.DAY_OF_MONTH))
					: "0" + String.valueOf(cstart.get(Calendar.DAY_OF_MONTH));
			String month = ((cstart.get(Calendar.MONTH) + 1) >= 10) ? String.valueOf((cstart.get(Calendar.MONTH) + 1))
					: "0" + String.valueOf((cstart.get(Calendar.MONTH) + 1));
			String date = cstart.get(Calendar.YEAR) + "-" + month + "-" + day;
			String year = String.valueOf(cstart.get(Calendar.YEAR));
			String weekofDay = cstart.get(Calendar.YEAR) + "-" + month + "-" + cstart.get(Calendar.WEEK_OF_MONTH);
			if (!this.yearContained.contains(year) && !this.monthContained.contains(year + "-" + month)
					&& !this.weekContained.contains(weekofDay)) {
				if (!intermediateResult.contains(date)) {
					intermediateResult.add(date);
				}
			}
			cstart.add(Calendar.DATE, 1);

		}

		// add the last day
//		String day = (cstart.get(Calendar.DAY_OF_MONTH) >= 10) ? String.valueOf(cstart.get(Calendar.DAY_OF_MONTH))
//				: "0" + String.valueOf(cstart.get(Calendar.DAY_OF_MONTH));
//		String month = ((cstart.get(Calendar.MONTH) + 1) >= 10) ? String.valueOf((cstart.get(Calendar.MONTH) + 1))
//				: "0" + String.valueOf((cstart.get(Calendar.MONTH) + 1));
//		String date = cstart.get(Calendar.YEAR) + "-" + month + "-" + day;
//		if (!intermediateResult.contains(date)) {
//			intermediateResult.add(date);
//		}

		return intermediateResult;
	}

	/**
	 * This method return all weeks between the start and the end date that
	 * represent time interval.
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
			String weekofDay = cstart.get(Calendar.YEAR) + "-" + month + "-" + cstart.get(Calendar.WEEK_OF_MONTH);
			if (!intermediateResult.contains(weekofDay)) {
				intermediateResult.add(weekofDay);
			}
			cstart.add(Calendar.DATE, 1);

		}
		return intermediateResult;
	}

	/**
	 * This method return all weeks fully contained between the start and the
	 * end date that represent time interval. This method should be called after
	 * these methods getYearContainedBy , and MonthContainedBy.
	 * 
	 * @param startDate
	 * @param endDate
	 * @return List<String> each in a format of yyyy-MM-w
	 * @throws ParseException
	 */
	public List<String> getWeekContainedBy(String startDate, String endDate) throws ParseException {
		Date start = dateFormat.parse(startDate);
		Date end = dateFormat.parse(endDate);
		Calendar cstart = Calendar.getInstance();
		Calendar cend = Calendar.getInstance();
		cstart.setTime(start);
		cend.setTime(end);
		List<String> intermediateResult = new ArrayList<String>();
		while (cstart.before(cend) || cstart.equals(cend)) {
			String month = ((cstart.get(Calendar.MONTH) + 1) >= 10) ? String.valueOf((cstart.get(Calendar.MONTH) + 1))
					: "0" + String.valueOf((cstart.get(Calendar.MONTH) + 1));
			String weekofDay = cstart.get(Calendar.YEAR) + "-" + month + "-" + cstart.get(Calendar.WEEK_OF_MONTH);
			String year = String.valueOf(cstart.get(Calendar.YEAR));
			if (!this.yearContained.contains(year) && !this.monthContained.contains(year + "-" + month)) {
				//
				Calendar temp = Calendar.getInstance();
				temp.setTime(cstart.getTime());
				temp.add(Calendar.DATE, -1);
				if (temp.get(Calendar.WEEK_OF_MONTH) != cstart.get(Calendar.WEEK_OF_MONTH)) {
					int daysinWeek = 1;
					temp.add(Calendar.DATE, 1);
					while(temp.get(Calendar.WEEK_OF_MONTH) == cstart.get(Calendar.WEEK_OF_MONTH)){
						temp.add(Calendar.DATE, 1);
					}
					temp.add(Calendar.DATE, -1);
					if (!temp.after(cend) && !intermediateResult.contains(weekofDay)) {
						intermediateResult.add(weekofDay);
					}
					
				}
			}
			cstart.add(Calendar.DATE, 1);

		}
		this.weekContained = intermediateResult;
		return intermediateResult;
	}

	/***
	 * This method return all months contains the time interval passed in the
	 * parameters.
	 * 
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
	 * This method return all months fully inside two date time range . e.g.,
	 * passing parameter 2017-02-01 , 2017-05-10 This method will return month
	 * 2017-02, 2017-03, 2017-04
	 * 
	 * @param startDate
	 * @param endDate
	 * @return List<String> each in a format of yyyy-MM
	 * @throws ParseException
	 */
	public List<String> getMonthsContainedBy(String startDate, String endDate) throws ParseException {
		List<String> result = new ArrayList<String>();
		Date start = dateFormat.parse(startDate);
		Date end = dateFormat.parse(endDate);
		Calendar c = Calendar.getInstance();
		// set start time one day before to make sure that the first datetime of
		// month inside range
		c.setTime(start);
		c.add(Calendar.DAY_OF_YEAR, -1);
		start = c.getTime();
		List<String> months = this.getMonth(startDate, endDate);
		// List<String> yearsContained = this.getYearContainedBy(startDate,
		// endDate);
		Date monthStart;
		Date monthEnd;

		String yearCheck;
		for (String month : months) {
			monthStart = dateFormat.parse(month + "-01");
			yearCheck = month.split("-")[0];
			if (!this.yearContained.contains(yearCheck)) {
				c.setTime(monthStart);
				c.set(Calendar.DAY_OF_MONTH, c.getActualMaximum(Calendar.DAY_OF_MONTH));
				monthEnd = c.getTime();
				if (start.before(monthStart) && end.after(monthEnd)) {
					result.add(month);
				}

			}
		}
		this.monthContained = result;
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

	/***
	 * This method return all years contains the time interval passed in the
	 * parameters.
	 * 
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
		while (start.getYear() <= end.getYear()) {
			result.add(String.valueOf(c.get(Calendar.YEAR)));
			c.add(Calendar.YEAR, 1);
			start = c.getTime();

		}
		return result;
	}

	/***
	 * This method return all years contains the time interval passed in the
	 * parameters.
	 * 
	 * @param startDate
	 * @param endDate
	 * @return List<String> each in a format of yyyy
	 * @throws ParseException
	 */
	public List<String> getYearContainedBy(String startDate, String endDate) throws ParseException {
		List<String> result = new ArrayList<String>();
		Date start = dateFormat.parse(startDate);
		Date end = dateFormat.parse(endDate);
		List<String> years = this.getYear(startDate, endDate);
		Date yearStart;
		Date yearEnd;
		for (String year : years) {
			yearStart = dateFormat.parse(year + "-01-01");
			Calendar cld = Calendar.getInstance();
			cld.setTime(yearStart);
			cld.add(Calendar.YEAR, 1);
			// cld.set(Calendar.DAY_OF_YEAR,1);
			cld.add(Calendar.DAY_OF_YEAR, -1); // last day of the year.
			yearEnd = cld.getTime();
			if (yearEnd.before(end) && yearStart.after(start)) {
				result.add(String.valueOf(cld.get(Calendar.YEAR)));
			}

		}
		// Finally Calculate if the %months in the beginning of time range is
		// more than %40
		// This is will be to do in the future. After developing the
		// SpatioTemporal Record Reader.
		this.yearContained = result;
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

		String from = "2014-01-02";
		String to = "2014-01-13";
		//
		// List<Path> result = l.getQueryPlan(from, to);
		// System.out.println("Result: \n");
		// for (Path x : result)
		// System.out.println(x.toString());

		List<String> result = null;

		result = l.getYear(from, to);
		System.out.println("Result year:");
		for (String x : result)
			System.out.println(x);

		result = l.getMonth(from, to);
		System.out.println("Result month:");
		for (String x : result)
			System.out.println(x);

		result = l.getWeek(from, to);
		System.out.println("Result week:");
		for (String x : result)
			System.out.println(x);

		result = l.getDay(from, to);
		System.out.println("Result Day: \n");
		for (String x : result)
			System.out.println(x);

		System.out.println("*****************************");
		result = l.getYearContainedBy(from, to);
		System.out.println("Result Year Contained: \n");
		for (String x : result)
			System.out.println(x);

		result = l.getMonthsContainedBy(from, to);
		System.out.println("Result monthContain: \n");
		for (String x : result)
			System.out.println(x);

		result = l.getWeekContainedBy(from, to);
		System.out.println("Result Weeks Contained: \n");
		for (String x : result)
			System.out.println(x);

		result = l.getDayContainedBy(from, to);
		System.out.println("Result days Contained: \n");
		for (String x : result)
			System.out.println(x);

	}
}
