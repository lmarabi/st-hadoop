package edu.umn.cs.sthadoop.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Louai Alarabi.
 */
public class Commons {

	public static String hadoopDir;
	public static String sthadoopJar;
	public static int portNumber;
	// point to the spatio-temporal index
	public static String queryIndex;
    

    public Commons() throws IOException {
        this.loadConfigFile();
       
    }
    
    public  int getPortNumber() {
        return portNumber;
    }

    public  void setPortNumber(int portNumber) {
        Commons.portNumber = portNumber;
    }

    public  String getHadoopDir() {
        return hadoopDir;
    }

    public  void setHadoopDir(String hadoopDir) {
        Commons.hadoopDir = hadoopDir;
    }

    
    public static String getSTHadoopJar() {
		return sthadoopJar;
	}
    
    public static void setShadoopJar(String shadoopJar) {
		Commons.sthadoopJar = shadoopJar;
	}
    
    public static void setQueryIndex(String queryIndex) {
		Commons.queryIndex = queryIndex;
	}
    
    public static String getQueryIndex() {
		return queryIndex;
	}
    
    
    private void loadConfigFile() throws IOException {

        Properties prop = new Properties();
        prop.load(new FileInputStream("config.properties"));
        Commons.hadoopDir = prop.getProperty("hadoopDir");
        Commons.sthadoopJar = prop.getProperty("sthadoopJar");
        Commons.portNumber = Integer.parseInt(prop.getProperty("portNumber"));
        Commons.queryIndex = prop.getProperty("queryIndex");
        System.out.println("Config file Loaded");


    }


}
