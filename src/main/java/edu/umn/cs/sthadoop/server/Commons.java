package edu.umn.cs.sthadoop.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Louai Alarabi.
 */
public class Commons {

	// point to the spatio-temporal index
	public static String nycIndex;
	public static String twitterIndex;
	public static String spatialIndex;
	public static String queryResult;
    

    public Commons() throws IOException {
        this.loadConfigFile();
       
    }
    
    public static void setNycIndex(String queryIndex) {
		Commons.nycIndex = queryIndex;
	}
    
    public static String getNycIndex() {
		return nycIndex;
	}
    
    public static String getTwitterIndex() {
		return twitterIndex;
	}
    
    public static String getSpatialIndex() {
		return spatialIndex;
	}
    
    public static void setTwitterIndex(String queryIndex) {
		Commons.twitterIndex = queryIndex;
	}
    
    public static void setSpatialIndex(String queryIndex) {
		Commons.spatialIndex = queryIndex;
	}
    
    public static void setQueryResult(String queryResult) {
		Commons.queryResult = queryResult;
	}
    
    public static String getQueryResult() {
		return queryResult;
	}
    
    
    private void loadConfigFile() throws IOException {

        Properties prop = new Properties();
        prop.load(new FileInputStream("config.properties"));
        Commons.nycIndex = prop.getProperty("nycIndex");
        Commons.twitterIndex = prop.getProperty("twitterIndex");
        Commons.spatialIndex = prop.getProperty("spatialIndex");
        Commons.queryResult = prop.getProperty("queryResult");
        System.out.println("Config file Loaded");


    }


}
