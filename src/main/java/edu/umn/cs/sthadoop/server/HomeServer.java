/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umn.cs.sthadoop.server;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.fs.Path;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import com.google.gson.stream.JsonWriter;

import edu.umn.cs.sthadoop.server.ServerRequest.queryShape;
import edu.umn.cs.sthadoop.server.ServerRequest.queryoperation;

/**
 *
 * @author turtle
 */
public class HomeServer extends AbstractHandler {
	
	public static ServerRequest serverRequester;
	
	
	
	@Override
	public void handle(String arg0, HttpServletRequest request, HttpServletResponse response, int arg3)
			throws IOException, ServletException {
		// TODO Auto-generated method stub
		executeRequest(arg0,request,response);
		
	}
	
	public void executeRequest(String arg,HttpServletRequest request, HttpServletResponse response) throws UnsupportedEncodingException, IOException{
		response.setStatus(HttpServletResponse.SC_OK);
		response.setContentType("text/html;charset=utf-8");
		response.addHeader("Access-Control-Allow-Origin", "*");
		response.addHeader("Content-Encoding", "gzip");
		response.addHeader("Access-Control-Allow-Credentials", "true");
		String path = request.getPathInfo();
		if (path.equals("/query")) {
			serverRequester.setRect(request.getParameter("x1"),request.getParameter("y1")
			,request.getParameter("x2"),request.getParameter("y2"));
			serverRequester.setT1(request.getParameter("t1"));
			serverRequester.setT2(request.getParameter("t2"));
			serverRequester.setOperation(queryoperation.valueOf(request.getParameter("operation")));
			serverRequester.setShape(queryShape.valueOf(request.getParameter("shape")));
			// query the data from the spatio-temporal index.
			List<Partition> stPartitions = new ArrayList<Partition>();
			List<Partition> sPartitions = new ArrayList<Partition>();
			List<Point> result = new ArrayList<Point>();
			long resultCount; // store the result count of the answer
			try {
				//First Get the spatio-temporal partitions
				stPartitions = serverRequester.getQueryPartitions(null);
				//Second Get the spatial Partitions
				sPartitions = serverRequester.ReadMaster(new Path(Commons.getQueryIndex()+"/all"));
				//serverRequester.executeQuery();
				resultCount = serverRequester.executeRangeQuery();
				System.out.println("Result count: "+resultCount);
				result = serverRequester.getFinalResult();
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// respond to the user throw jason 
			JsonWriter writer = new JsonWriter(new OutputStreamWriter(
					new GZIPOutputStream(response.getOutputStream()),
					"UTF-8"));
			writer.setLenient(true);
			/*
			 * Get the spatio-temporal partitions 
			 */
			writer.beginObject();
			writer.name("STPartitions");
			writer.beginArray();
			for(Partition part : stPartitions){
				writer.beginObject();
				writer.name("day").value(part.getDay());
				writer.name("mbr").value(part.getArea().toString());
				writer.name("cardinality").value(part.getCardinality());
				writer.endObject();
			}
			writer.endArray();
			writer.endObject();
			/*
			 * Get the spatial partitions information
			 */
			writer.beginObject();
			writer.name("SPartitions");
			writer.beginArray();
			for(Partition part : sPartitions){
				writer.beginObject();
				writer.name("day").value(part.getDay());
				writer.name("mbr").value(part.getArea().toString());
				writer.name("cardinality").value(part.getCardinality());
				writer.endObject();
			}
			writer.endArray();
			writer.endObject();
			/*
			 * Get the actual data for visualization
			 */
			writer.beginObject();
			writer.name("data");
			writer.beginArray();
			for(Point p : result){
				writer.beginObject();
				writer.name("x").value(p.getX());
				writer.name("y").value(p.getY());
				writer.endObject();
			}
			writer.endArray();
			writer.endObject();

			
			
			writer.close();
			
		} else {
			response.getWriter()
			.print("<h2> Welcome to this tutorial </h2> <br /><br />"
					+ "These are the main functionalties that are implemeneted in this server <br /><br />"
					+ "<a href='http://localhost:8085/query?operation=rq&shape=stpoint&x1=-180&y2=-90&x2=180&y2=90&t1=2015-01-01&t2=2015-01-02'>http://localhost:8085/query?operation=rq&shape=stpoint&x1=-180&y2=-90&x2=180&y2=90&t1=2015-01-01&t2=2015-01-02</a><br /><br />"
					+ "<h1> Thank You </h1>");
}
	}
	


	public static void main(String[] args) throws Exception {
		Server server = new Server(8085);
		serverRequester = new ServerRequest();
		server.setHandler(new HomeServer());
		server.start();
		server.join();
	}

	

}
