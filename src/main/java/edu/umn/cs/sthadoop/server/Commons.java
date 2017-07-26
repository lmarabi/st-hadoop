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
	public static String spatialTwitterIndex;
	public static String spatialTaxiIndex;
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
    
    public static String getSpatialTwitterIndex() {
		return spatialTwitterIndex;
	}
    
    public static void setTwitterIndex(String queryIndex) {
		Commons.twitterIndex = queryIndex;
	}
    
    public static void setSpatialTwitterIndex(String queryIndex) {
		Commons.spatialTwitterIndex = queryIndex;
	}
    
    public static void setSpatialTaxiIndex(String queryIndex) {
		Commons.spatialTaxiIndex = queryIndex;
	}
    
    public static String getSpatialTaxiIndex() {
		return spatialTaxiIndex;
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
        Commons.spatialTwitterIndex = prop.getProperty("spatialTwitterIndex");
        Commons.spatialTaxiIndex = prop.getProperty("spatialTaxiIndex");
        Commons.queryResult = prop.getProperty("queryResult");
        System.out.println("Config file Loaded");


    }


}
