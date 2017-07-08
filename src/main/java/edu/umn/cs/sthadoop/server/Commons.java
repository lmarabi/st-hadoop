package edu.umn.cs.sthadoop.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Louai Alarabi.
 */
public class Commons {

	// point to the spatio-temporal index
	public static String queryIndex;
	public static String queryResult;
    

    public Commons() throws IOException {
        this.loadConfigFile();
       
    }
    
    public static void setQueryIndex(String queryIndex) {
		Commons.queryIndex = queryIndex;
	}
    
    public static String getQueryIndex() {
		return queryIndex;
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
        Commons.queryIndex = prop.getProperty("queryIndex");
        Commons.queryResult = prop.getProperty("queryResult");
        System.out.println("Config file Loaded");


    }


}
