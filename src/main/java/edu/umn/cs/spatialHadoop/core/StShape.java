package edu.umn.cs.spatialHadoop.core;

import java.awt.Graphics;

import org.apache.hadoop.io.Writable;
import edu.umn.cs.spatialHadoop.io.TextSerializable;

/**
 * A general 1D shape
 * @author Louai Alarabi
 *
 */

public interface StShape extends Writable, Cloneable, TextSerializable {
	 /**
	   * Returns minimum bounding Interval for this shape.
	   * @return The minimum bounding Interval for this shape
	   */
	  public Interval getMBR();
	  
	  /**
	   * Gets the distance of this shape to the given two time interval.
	   * @param t1  The start timeStamp of the object. 
	   * @param t1  The end timeStamp of the object.
	   * @return The number of intervals between t1 and t2 
	   */
	  public long distanceTo(long t1, long t2);
	  
	  /**
	   * Returns true if this shape is intersected with the given shape
	   * @param s The other shape to test for intersection with this shape
	   * @return <code>true</code> if this shape intersects with s; <code>false</code> otherwise.
	   */
	  public boolean isIntersected(final StShape s);
	  
	  /**
	   * Returns a clone of this shape
	   * @return A new object which is a copy of this shape
	   */
	  public StShape clone();
	  
	  /**
	   * Draws a shape to the given graphics.
	   * @param g The graphics or canvas to draw to
	   * @param fileMBR the MBR of the file in which the shape is contained
	   * @param imageWidth width of the image to draw
	   * @param imageHeight height of the image to draw
	   * @param scale the scale used to convert shape coordinates to image coordinates
	   * @deprecated Please use {@link #draw(Graphics, double, double)}
	   */
	  @Deprecated
	  public void draw(Graphics g, Interval fileMBR, int imageWidth, int imageHeight, double scale);
	  
	  /**
	   * Draws the shape to the given graphics and scale.
	   * @param g - the graphics to draw the shape to.
	   * @param xscale - scale of the image x-axis in terms of pixels per points.
	   * @param yscale - scale of the image y-axis in terms of pixels per points.
	   */
	  public void draw(Graphics g, double xscale, double yscale);

	int compareTo(StShape s);
	}
