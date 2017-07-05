/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umn.cs.sthadoop.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.operations.RangeQuery;
import edu.umn.cs.sthadoop.core.QueryPlanner;
import edu.umn.cs.sthadoop.core.STPoint;
import edu.umn.cs.sthadoop.operations.STRangeQuery;
import edu.umn.cs.sthadoop.operations.TestSTRQ;

/**
 *
 * @author Louai Alarabi
 */
/**
 * @author louai
 *
 */
public class ServerRequest {



	
	private queryShape shape;
	private queryoperation operation;
	private String x1; 
	private String y1; 
	private String x2; 
	private String y2;
	private String t1; 
	private String t2;

	public String getX1() {
		return x1;
	}

	public void setX1(String x1) {
		this.x1 = x1;
	}

	public String getY1() {
		return y1;
	}

	public void setY1(String y1) {
		this.y1 = y1;
	}

	public String getX2() {
		return x2;
	}

	public void setX2(String x2) {
		this.x2 = x2;
	}

	public String getY2() {
		return y2;
	}

	public void setY2(String y2) {
		this.y2 = y2;
	}

	public String getT1() {
		return t1;
	}

	public void setT1(String t1) {
		this.t1 = t1;
	}

	public String getT2() {
		return t2;
	}

	public void setT2(String t2) {
		this.t2 = t2;
	}

	public enum queryShape {

		stpoint, twitter
	};

	public enum queryoperation {

		rq, join, knn
	};

	public queryShape getShape() {
		return shape;
	}

	public void setShape(queryShape shape) {
		this.shape = shape;
	}

	public queryoperation getOperation() {
		return operation;
	}

	public void setOperation(queryoperation operation) {
		this.operation = operation;
	}

	public ServerRequest() throws FileNotFoundException, IOException, ParseException {
	}

	public void executeRQ() throws Exception {
		String[] args = new String[5];	
		args[0] = Commons.getQueryIndex();
		args[1] = "/home/louai/nyc-taxi/resultSTRQ";
		if(shape.equals(queryShape.twitter)){
			args[2] = "shape:edu.umn.cs.sthadoop.core.STpointsTweets";
		}else{
			args[2] = "shape:edu.umn.cs.sthadoop.core.STPoint";
		}
		args[3] = "rect:"+x1+","+y1+","+x2+","+y2;
		args[4] = "interval:"+ t1 + ","+ t2;
		STRangeQuery.main(args);
		
	}
	
	

}
