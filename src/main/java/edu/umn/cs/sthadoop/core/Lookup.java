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


/**
 *
 * @author louai
 */
public class Lookup {

	private List<String> dayDatesTweet = new ArrayList<String>();
	private List<String> monthDatesTweet = new ArrayList<String>();
	private List<String> monthPathsTweet = new ArrayList<String>();

	private List<String> dayPathsTweet = new ArrayList<String>();

	private List<Week> weekDatesTweet = new ArrayList<Week>();
	private List<String> weekPathsTweet = new ArrayList<String>();

	public static SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyy-MM-dd");
	private Map<String, String> dayLookupTweet = new HashMap<String, String>();
	private Map<String, String> weekLookupTweet = new HashMap<String, String>();
	private List<Date> missingDays = new ArrayList<Date>();
	private String dirPath;

	public Lookup() {
	}

	/**
	 * This method will load the lookupTable to the memory
	 *
	 * @param path
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void loadLookupTableToArrayList(String path)
			throws FileNotFoundException, IOException, ParseException {
		// ************************ Load missing days
		dirPath = path;
		String missing_Day_file = path + "/tweets/Day/_missing_Days.txt";
		System.out.println("Load missing days into memory");
		if (new File(missing_Day_file).exists()) {
			BufferedReader reader = new BufferedReader(new FileReader(
					missing_Day_file));
			String line = null;
			while ((line = reader.readLine()) != null) {
				missingDays.add(dateFormat.parse(line));

			}
			reader.close();
		}
		// ************************ Day lookup tables
		String tweetsLookup = path + "/tweets/Day/lookupTable.txt";
		System.out.println("Load lookup tables into memory");
		BufferedReader reader = new BufferedReader(new FileReader(tweetsLookup));
		String line = null;
		while ((line = reader.readLine()) != null) {
			if(line.matches(".*[1-9].*")){
			dayDatesTweet.add(line);
			dayPathsTweet.add(path + "/tweets/Day/index." + line);
			}

		}
		reader.close();

		// ************************ Week lookup tables
		tweetsLookup = path + "/tweets/Week/lookupTable.txt";
		reader = new BufferedReader(new FileReader(tweetsLookup));
		line = null;
		while ((line = reader.readLine()) != null) {
			if(line.matches(".*[1-9].*")){
				weekDatesTweet.add(new Week(line));
				weekPathsTweet.add(path + "/tweets/Week/index." + line);
			}

		}
		reader.close();

		// ********** Load lookup for Months
		tweetsLookup = path + "/tweets/Month/lookupTable.txt";
		reader = new BufferedReader(new FileReader(tweetsLookup));
		line = null;
		while ((line = reader.readLine()) != null) {
			if (line.matches(".*[1-9].*")) {
				monthDatesTweet.add(line);
				monthPathsTweet.add(path + "/tweets/Month/index." + line);
			}

		}
		reader.close();

	}

	/**
	 * This method will load the lookupTable to the memory
	 *
	 * @param path
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void loadLookupTableHashMap(String path)
			throws FileNotFoundException, IOException {
		String tweetsLookup = path + "/tweets/lookupTable.txt";
		BufferedReader reader = new BufferedReader(new FileReader(tweetsLookup));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String[] temp = line.split(",");
			dayLookupTweet.put(temp[0], temp[1]);
		}
		reader.close();
	}

	/**
	 * Print the content of the lookupTables
	 *
	 * @param tweet
	 * @param hashtag
	 */
	public void printlookUp() {

		Iterator iterator = dayLookupTweet.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry obj = (Map.Entry) iterator.next();
			System.out
					.println(obj.getKey().toString() + " , " + obj.getValue());
		}

	}

	/**
	 * Print LookupTable
	 */
	public void PrintLookupArrayList() {
		System.out.println("Date-Tweet");
		for (Iterator<String> it = dayDatesTweet.iterator(); it.hasNext();) {
			String d = it.next();
			System.out.println(d.toString());
		}

	}

	public List<String> getDayDatesTweet() {
		return dayDatesTweet;
	}

	public List<Week> getWeekDatesTweet() {
		return weekDatesTweet;
	}

	public List<String> getMonthDatesTweet() {
		return monthDatesTweet;
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
	public Map<String, String> getTweetsDayIndex(String startDate,
			String endDate) throws ParseException {
		Map<String, String> result = new HashMap<String, String>();
		for (int i = 0; i < dayDatesTweet.size(); i++) {
			if (insideDaysBoundry(startDate, endDate, dayDatesTweet.get(i))) {
				result.put(dayDatesTweet.get(i), dayPathsTweet.get(i));
			}
		}
		return result;
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
	public Map<Week, String> getAllTweetsWeekIndex(String startDate,
			String endDate) throws ParseException {
		Map<Week, String> result = new HashMap<Week, String>();
		for (int i = 0; i < weekDatesTweet.size(); i++) {
			if (insideDaysBoundry(startDate, endDate, dayDatesTweet.get(i))) {
				result.put(weekDatesTweet.get(i), weekPathsTweet.get(i));
			}
		}
		return result;
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
	public Map<Week, String> getTweetsWeekIndex(String startDate, String endDate)
			throws ParseException {
		Date start = dateFormat.parse(startDate);
		Date end = dateFormat.parse(endDate);
		Calendar cstart = Calendar.getInstance();
		Calendar cend = Calendar.getInstance();
		cstart.setTime(start);
		cend.setTime(end);
		Map<Week, String> result = new HashMap<Week, String>();
		List<String> intermediateResult = new ArrayList<String>();
		while(!(cstart.get(Calendar.YEAR) == cend.get(Calendar.YEAR) && 
				(cstart.get(Calendar.MONTH) == cend.get(Calendar.MONTH)) &&
						(cstart.get(Calendar.DATE) == cend.get(Calendar.DATE))
				)){
			String weekofDay = cstart.get(Calendar.YEAR)+"-"+(cstart.get(Calendar.MONTH)+1)+"-"+cstart.get(Calendar.WEEK_OF_MONTH);
			if(!intermediateResult.contains(weekofDay)){
				intermediateResult.add(weekofDay);
			}
			cstart.add(Calendar.DATE, 1);
			
		}
		
		//send result as hash map
		for(String week : intermediateResult){
			result.put(new Week(week), this.dirPath + "/tweets/Week/index."+week);
		}
		return result;
	}

	public List<String> getTweetsMonth(String startDate, String endDate)
			throws ParseException {
		List<String> result = new ArrayList<String>();
		Date start = dateFormat.parse(startDate);
		Date end = dateFormat.parse(endDate);
		Calendar c = Calendar.getInstance();
		c.setTime(start);
		String month;
		while (start.getYear() != end.getYear()
				|| start.getMonth() != end.getMonth()) {
			month = ((start.getMonth() + 1) >= 10) ? String.valueOf(start
					.getMonth() + 1) : "0"
					+ String.valueOf(start.getMonth() + 1);
			result.add(c.get(Calendar.YEAR) + "-" + month);
			c.add(Calendar.MONTH, 1);
			start = c.getTime();

		}
		month = ((start.getMonth() + 1) >= 10) ? String.valueOf(start
				.getMonth() + 1) : "0" + String.valueOf(start.getMonth() + 1);
		result.add(c.get(Calendar.YEAR) + "-" + month);
		return result;
	}

	public Map<String, String> getTweetsMonthwithDir(String startDate, String endDate)
			throws ParseException {
		Map<String, String> result = new HashMap<String, String>();
		Date start = dateFormat.parse(startDate);
		Date end = dateFormat.parse(endDate);
		Calendar c = Calendar.getInstance();
		c.setTime(start);
		String month;
		while (start.getYear() != end.getYear()
				|| start.getMonth() != end.getMonth()) {
			month = ((start.getMonth() + 1) >= 10) ? String.valueOf(start
					.getMonth() + 1) : "0"
					+ String.valueOf(start.getMonth() + 1);
			result.put(c.get(Calendar.YEAR) + "-" + month, this.dirPath
					+ "/tweets/Month/index." + c.get(Calendar.YEAR) + "-" + month);
			c.add(Calendar.MONTH, 1);
			start = c.getTime();

		}
		month = ((start.getMonth() + 1) >= 10) ? String.valueOf(start
				.getMonth() + 1) : "0" + String.valueOf(start.getMonth() + 1);
		result.put(c.get(Calendar.YEAR) + "-" + month, this.dirPath
				+ "/tweets/Month/index." + c.get(Calendar.YEAR) + "-" + month);
		return result;
	}

	/**
	 * This method return Full months between two dates
	 *
	 * @param startDate
	 * @param endDate
	 * @return
	 * @throws ParseException
	 */
	public Map<String, String> getTweetsMonthsIndex(String startDate,
			String endDate) throws ParseException {
		Map<String, String> result = new HashMap<String, String>();
		Date start = dateFormat.parse(startDate);
		Date end = dateFormat.parse(endDate);
		Calendar c = Calendar.getInstance();
		c.setTime(start);
		List<String> months = new ArrayList<String>();
		while (!start.equals(end)) {
			if (start.getMonth() != end.getMonth()) {
				if (start.getDate() == 1) {
					// System.out.println("Month found" + (start.getMonth() +
					// 1));
					String[] temp = Week.dateFormat.format(start).split("-");
					// add only month to temp
					months.add(temp[0] + "-" + temp[1]);
				}
			}else if((start.getMonth() == end.getMonth())&& (start.getYear() == end.getYear())){
				String[] temp = Week.dateFormat.format(start).split("-");
				// add only month to temp
				months.add(temp[0] + "-" + temp[1]);
			}
			c.add(Calendar.DATE, 1);
			start = c.getTime();
		}

		for (int j = 0; j < months.size(); j++) {
			for (int i = 0; i < monthDatesTweet.size(); i++) {
				if (months.get(j).equals(monthDatesTweet.get(i))) {
					result.put(months.get(j), monthPathsTweet.get(i));
				}
			}
		}
		return result;
	}

	/**
	 * This method return startdate and endDate of a begining range For example
	 * 2014-05-13 to 2014-08-01 then Method will return String[] range =
	 * {2014-05-13,2014-05-31}
	 *
	 * @param startDate
	 * @param endDate
	 * @return
	 * @throws ParseException
	 */
	public String[] getHeadofSubMonth(String startDate, String endDate)
			throws ParseException {
		Date start = dateFormat.parse(startDate);
		Date end = dateFormat.parse(endDate);
		Calendar c = Calendar.getInstance();
		String[] result = new String[2];
		List<Date> queried = new ArrayList<Date>();
		// Query for months
		Map<String, String> queriedM = this.getTweetsMonthsIndex(startDate,
				endDate);
		Iterator it = queriedM.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry obj = (Map.Entry) it.next();
			queried.add(dateFormat.parse(new String(obj.getKey() + "-1")));
		}
		// Check if the head of the month already queried in months then return
		// null
		if (queried.size() > 0) {
			Collections.sort(queried);
			Date headMonths = queried.get(0);
			if (headMonths.equals(start)) {
				return result;
			}
		}
		// IF the head did not queried in months
		c.setTime(start);
		String sRange = null;
		String eRange = null;
		List<String> months = new ArrayList<String>();
		while (!start.equals(end)) {
			if (start.getMonth() != end.getMonth()) {
				if (start.getDate() == 1) {
					// System.out.println("Month found"+(start.getMonth()+1));
					String[] temp = Week.dateFormat.format(start).split("-");
					// add only month to temp
					months.add(temp[0] + "-" + temp[1]);
					// add the previous range
					if (sRange != null) {
						c.add(Calendar.DATE, -1);
						Date erangeDate = c.getTime();
						eRange = Week.dateFormat.format(erangeDate);
						result[1] = eRange;
						return result;
					}

				} else {
					if (sRange == null) {
						sRange = Week.dateFormat.format(start);
						result[0] = sRange;
					}
				}
			}
			c.add(Calendar.DATE, 1);
			start = c.getTime();
		}
		return result;
	}

	/**
	 * This method return startdate and endDate of a begining range For example
	 * 2014-05-13 to 2014-08-01 then Method will return String[] range =
	 * {2014-05-13,2014-05-31}
	 *
	 * @param startDate
	 * @param endDate
	 * @return
	 * @throws ParseException
	 */
	public String[] getTailofSubMonth(String startDate, String endDate)
			throws ParseException {
		String[] result = new String[2];
		List<Date> queried = new ArrayList<Date>();
		Date start = dateFormat.parse(startDate);
		Date end = dateFormat.parse(endDate);
		Calendar c = Calendar.getInstance();
		c.setTime(start);
		String sRange = null;
		String eRange = null;
		List<String> months = new ArrayList<String>();
		// Query for months
		Map<String, String> queriedM = this.getTweetsMonthsIndex(startDate,
				endDate);
		Iterator it = queriedM.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry obj = (Map.Entry) it.next();
			Date firstDayofMonth = dateFormat.parse(new String(obj.getKey()
					+ "-1"));
			c.setTime(firstDayofMonth);
			c.add(Calendar.MONTH, 1);
			c.set(Calendar.DAY_OF_MONTH, 1);
			c.add(Calendar.DATE, -1);
			Date lastDay = c.getTime();
			queried.add(lastDay);
		}
		Collections.sort(queried);
		if (queried.size() > 0) {
			Date tailDate = queried.get(queried.size() - 1);
			if (!tailDate.equals(end)) {
				c.setTime(tailDate);
				c.add(Calendar.DATE, 1);
				result[0] = dateFormat.format(c.getTime());
				result[1] = dateFormat.format(end);
				return result;
			}
		}
		while (!start.equals(end)) {
			if (start.getDate() == 1) {
				// System.out.println("Month found"+(start.getMonth()+1));
				String[] temp = Week.dateFormat.format(start).split("-");
				// add only month to temp
				months.add(temp[0] + "-" + temp[1]);
				// add the previous range
				Date tempdate = c.getTime();
				sRange = Week.dateFormat.format(tempdate);
				result[0] = sRange;

			}
			c.add(Calendar.DATE, 1);
			start = c.getTime();
		}
		eRange = Week.dateFormat.format(start);
		result[1] = eRange;
		if (Week.dateFormat.parse(result[1]).getMonth() == start.getMonth()) {
			result = new String[2];
			return result;
		} else {
			return result;
		}
	}

//	/**
//	 * Get the missing tweets lookup Map<Date,String>
//	 *
//	 * @param startDate
//	 * @param endDate
//	 * @return
//	 * @throws ParseException
//	 */
//	public Map<String, String> getTweetMissingDaysinWeek(String startDate,
//			String endDate) throws ParseException {
//		Map<String, String> result = new HashMap<String, String>();
//		Date start = dateFormat.parse(startDate);
//		Date end = dateFormat.parse(endDate);
//		Map<Week, String> weekcovred = this.getTweetsWeekIndex(startDate,
//				endDate);
//		Map<String, String> days = this.getTweetsDayIndex(startDate, endDate);
//		Calendar c = Calendar.getInstance();
//		boolean cover = false;
//		while (!start.after(end)) {
//			Iterator it = weekcovred.entrySet().iterator();
//			while (it.hasNext()) {
//				Map.Entry obj = (Map.Entry) it.next();
//				Week week = (Week) obj.getKey();
//				// check and add to the list
//				if (week.isDayIntheWeek(start)) {
//					cover = true;
//				}
//
//			}
//			if (!cover) {
//				if (days.get(start) != null) {
//					result.put(dateFormat.format(start),
//							days.get(dateFormat.format(start)));
//				}
//			} else {
//				cover = false;
//			}
//			c.setTime(start);
//			c.add(Calendar.DATE, 1); // number of days to add
//			start = c.getTime();
//		}
//		return result;
//	}

	/**
	 * Return the dayLookupTweet table
	 *
	 * @return
	 */
	public Map<String, String> getTweetLookup() {
		return dayLookupTweet;
	}

	public String getTweetsPath(String key) {
		return dayLookupTweet.get(key);
	}

	public boolean isTweetsExist(String key) {
		return dayLookupTweet.containsKey(key);
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
	public static boolean insideDaysBoundry(String start, String end,
			String lookupStringDate) throws ParseException {
		Date startDate = dateFormat.parse(start);
		Date endDate = dateFormat.parse(end);
		Date lookupDate = dateFormat.parse(lookupStringDate);
		if ((lookupDate.compareTo(startDate) >= 0)
				&& (lookupDate.compareTo(endDate) <= 0)) {
			return true;
		}
		return false;
	}

//	public static boolean insideWeekBoundry(String start, String end, Week range)
//			throws ParseException {
//		if (insideDaysBoundry(start, end, range.getStart())
//				&& insideDaysBoundry(start, end, range.getEnd())) {
//			return true;
//		}
//		return false;
//	}

	/***
	 * This method check if day exist in the missing day list or not
	 * 
	 * @param day
	 * @return true if the day has complete dataset and False if the day miss
	 *         some data
	 * @throws ParseException
	 */
	public boolean isDayFromMissingDay(String day) throws ParseException {
		if (!day.matches("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]")) {
			return false;
		}
		Date temp = dateFormat.parse(day);
		return missingDays.contains(temp);
	}

	public static void main(String[] args) throws FileNotFoundException,
			IOException, ParseException {
		Lookup l = new Lookup();
		String path = "/home/turtle/UQUGIS/taghreed/Tools/twittercrawlermavenproject/output/result/";
		l.loadLookupTableToArrayList(path);
		String start = "2013-10-01";
		String end = "2013-12-01";
		Map<String, String> result = l.getTweetsMonthsIndex(start, end);
		System.out.println("Selected Months");
		Iterator it = result.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry obj = (Map.Entry) it.next();
			System.out.println(obj.getValue());
		}

		System.out
				.println("********* Missing Head non month Range *************");
		for (String t : l.getHeadofSubMonth(start, end)) {
			System.out.println(t);
		}

		System.out
				.println("********* Missing  tail non month range *************");
		for (String t : l.getTailofSubMonth(start, end)) {
			System.out.println(t);
		}
		// Map<Date,String> days = l.getTweetMissingDaysinWeek(start, end);
		// Iterator itdyas = days.entrySet().iterator();
		// while(itdyas.hasNext()){
		// Map.Entry obj = (Map.Entry)itdyas.next();
		// Date temp = (Date) obj.getKey();
		// System.out.println(temp+"\n"+obj.getValue());
		// }

	}
}
