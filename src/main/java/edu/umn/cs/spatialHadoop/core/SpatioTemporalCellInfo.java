/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;


/**
 * Information about a specific cell in a grid.
 * Note: Whenever you change the instance variables that need to
 * be stored in disk, you have to manually fix the implementation of class
 * BlockListAsLongs
 * @author Louai Alarabi
 *
 */
public class SpatioTemporalCellInfo extends Interval {
  
  /**
   * A unique ID for this cell in a file. This must be set initially when
   * cells for a file are created. It cannot be guessed from cell dimensions.
   */
  public int cellId;

  /**
   * Loads a cell serialized to the given stream
   * @param in
   * @throws IOException 
   */
  public SpatioTemporalCellInfo(DataInput in) throws IOException {
    this.readFields(in);
  }

  public SpatioTemporalCellInfo(String in) {
    this.fromText(new Text(in));
  }

  public SpatioTemporalCellInfo() {
    super();
  }

  public SpatioTemporalCellInfo(int id, long t1, long t2) {
    super(t1,t2);
    this.cellId = id;
  }

  public SpatioTemporalCellInfo(int id, Interval cellInfo) {
    this(id, cellInfo.t1, cellInfo.t2);
    if (id == 0)
      throw new RuntimeException("Invalid cell id: "+id);
  }
  
  public SpatioTemporalCellInfo(SpatioTemporalCellInfo c) {
    this.set(c);
  }

  public void set(SpatioTemporalCellInfo c) {
    if (c == null) {
      this.cellId = 0; // Invalid number
    } else {
      super.set(c); // Set rectangle
      this.cellId = c.cellId; // Set cellId
    }
  }
  
  @Override
  public String toString() {
    return "Cell #"+cellId+" "+super.toString();
  }
  
  @Override
  public SpatioTemporalCellInfo clone() {
    return new SpatioTemporalCellInfo(cellId, t1,t2);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    return ((SpatioTemporalCellInfo)obj).cellId == this.cellId;
  }
  
  @Override
  public int hashCode() {
    return (int) this.cellId;
  }
  
  @Override
  public int compareTo(StShape s) {
    return (int) (this.cellId - ((SpatioTemporalCellInfo)s).cellId);
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(cellId);
    super.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.cellId = in.readInt();
    super.readFields(in);
  }

  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeInt(cellId, text, ',');
    return super.toText(text);
  }
  
  @Override
  public void fromText(Text text) {
    this.cellId = TextSerializerHelper.consumeInt(text, ',');
    super.fromText(text);
  }
  
}
