/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umn.cs.sthadoop.server;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import org.mortbay.jetty.Request;
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
			serverRequester.setX1(request.getParameter("x1"));
			serverRequester.setY1(request.getParameter("y1"));
			serverRequester.setX2(request.getParameter("x2"));
			serverRequester.setY2(request.getParameter("y2"));
			serverRequester.setT1(request.getParameter("t1"));
			serverRequester.setT2(request.getParameter("t2"));
			serverRequester.setOperation(queryoperation.valueOf(request.getParameter("operation")));
			serverRequester.setShape(queryShape.valueOf(request.getParameter("shape")));
			// query the data from the spatio-temporal index. 
			
			// respond to the user throw jason 
			JsonWriter writer = new JsonWriter(new OutputStreamWriter(
					new GZIPOutputStream(response.getOutputStream()),
					"UTF-8"));
			writer.setLenient(true);
			writer.beginObject();
			writer.name("result");
			writer.beginArray();
			writer.beginObject();
			writer.name("hello").value(1);
			writer.endObject();
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
