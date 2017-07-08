/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umn.cs.sthadoop.server;

import java.io.File;

/**
 *
 * @author Louai Alarabi
 */
public class Partition {
	private File partition;
	private MBR area;
	private long cardinality;
	private double persent;
	private String Day;

	public Partition() {
	}

	// public Partition(File partition, MBR area) {
	// this.partition = partition;
	// this.area = area;
	// }

	public Partition(String line, String path, String day) {
		String[] temp = line.split(",");
		if (temp.length == 8) {
			Point pointMin = new Point(temp[1], temp[2]);
			Point pointMax = new Point(temp[3], temp[4]);
			this.area = new MBR(pointMax, pointMin);
			this.partition = new File(path + "/" + temp[7]);
			this.cardinality = Long.parseLong(temp[5]);
			this.Day = day;
		}
	}

	public File getPartition() {
		return partition;
	}

	public void setPartition(File partition) {
		this.partition = partition;
	}

	public MBR getArea() {
		return area;
	}

	public void setArea(MBR area) {
		this.area = area;
	}

	public void setCardinality(long cardinality) {
		this.cardinality = cardinality;
	}

	public void setPersent(double persent) {
		this.persent = persent;
	}

	public double getPersent() {
		return persent;
	}

	/**
	 * Cardinality of the partition is the number of rows in this partition
	 * 
	 * @return
	 */
	public long getCardinality() {
		return cardinality;
	}

	public String partitionToWKT() {
		return this.Day + "\tPOLYGON ((" + this.area.getMax().getY() + " " + this.area.getMin().getX() + ", "
				+ this.area.getMax().getY() + " " + this.area.getMax().getX() + ", " + this.area.getMin().getY() + " "
				+ this.area.getMax().getX() + ", " + this.area.getMin().getY() + " " + this.area.getMin().getX() + ", "
				+ this.area.getMax().getY() + " " + this.area.getMin().getX() + "))\t" + this.cardinality;
	}

}
