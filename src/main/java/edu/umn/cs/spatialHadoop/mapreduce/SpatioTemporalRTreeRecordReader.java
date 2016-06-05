/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapreduce;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Interval;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.core.StShape;
import edu.umn.cs.spatialHadoop.indexing.GlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.Partition;
import edu.umn.cs.spatialHadoop.indexing.RTree;
import edu.umn.cs.spatialHadoop.indexing.SpatioTemporalGlobalIndex;
import edu.umn.cs.spatialHadoop.indexing.SpatioTemporalPartition;


/**
 * Reads a file that contains R-trees.
 * @author Louai
 *
 */
public class SpatioTemporalRTreeRecordReader<V extends StShape> extends
    RecordReader<Partition, Iterable<V>> {

  private static final Log LOG = LogFactory.getLog(SpatioTemporalRTreeRecordReader.class);
  
  /**The codec used with the input file*/
  private CompressionCodec codec;
  /**The decompressor (instance) used to decompress the input file*/
  private Decompressor decompressor;

  /** File system of the file being parsed */
  private FileSystem fs;
  /**The path of the input file to read*/
  private Path path;
  /**The offset to start reading the raw (uncompressed) file*/
  private long start;
  /**The last byte to read in the raw (uncompressed) file*/
  private long end;
  
  /** The boundary of the partition currently being read */
  protected SpatioTemporalPartition cellMBR;
  
  /**
   * The input stream that reads directly from the input file.
   * If the file is not compressed, this stream is the same as #in.
   * Otherwise, this is the raw (compressed) input stream. This stream is used
   * only to calculate the progress of the input file.
   */
  private FSDataInputStream directIn;
  /** Input stream that reads data from input file */
  private DataInputStream in;
  /**An object that is used to read the current file position*/
  private Seekable filePosition;

  /**The shape used to parse input lines*/
  private V stockShape;
  
  /**Start offset of the next tree*/
  private long offsetOfNextTree;
  
  /**Value to be returned*/
  private Iterable<V> value;

  /**Optional query range*/
  private StShape inputQueryRange;
  /**The MBR of the input query. Used to apply duplicate avoidance technique*/
  private Interval inputQueryMBR;

  public SpatioTemporalRTreeRecordReader() {
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context != null? context.getConfiguration() : new Configuration();
    initialize(split, conf);
  }

  public void initialize(InputSplit split, Configuration conf)
      throws IOException, InterruptedException {
    LOG.info("Open a SpatialRecordReader to split: "+split);
    FileSplit fsplit = (FileSplit) split;
    this.path = fsplit.getPath();
    this.start = fsplit.getStart();
    this.end = this.start + split.getLength();
    this.fs = this.path.getFileSystem(conf);
    this.directIn = fs.open(this.path);
    codec = new CompressionCodecFactory(conf).getCodec(this.path);
    
    if (codec != null) {
      // Input is compressed, create a decompressor to decompress it
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        // A splittable compression codec, can seek to the desired input pos
        final SplitCompressionInputStream cIn =
            ((SplittableCompressionCodec)codec).createInputStream(
                directIn, decompressor, start, end,
                SplittableCompressionCodec.READ_MODE.BYBLOCK);
        in = new DataInputStream(cIn);
        start = cIn.getAdjustedStart();
        end = cIn.getAdjustedEnd();
        // take pos from compressed stream as we adjusted both start and end
        // to match with the compressed file
        filePosition = cIn;
      } else {
        // Non-splittable input, need to start from the beginning
        CompressionInputStream cIn = codec.createInputStream(directIn, decompressor);
        in = new DataInputStream(cIn);
        filePosition = cIn;
      }
    } else {
      // Non-compressed file, seek to the desired position and use this stream
      // to get the progress and position
      directIn.seek(start);
      in = directIn;
      filePosition = directIn;
    }
    byte[] signature = new byte[8];
    in.readFully(signature);
    if (!Arrays.equals(signature, SpatialSite.RTreeFileMarkerB)) {
      throw new RuntimeException("Incorrect signature for RTree");
    }
    this.stockShape = (V) OperationsParams.getShape(conf, "shape");

    if (conf.get(SpatialInputFormat3.InputQueryRange) != null) {
      // Retrieve the input query range to apply on all records
      this.inputQueryRange = OperationsParams.getStShape(conf,
          SpatialInputFormat3.InputQueryRange);
      this.inputQueryMBR = this.inputQueryRange.getMBR();
    }

    // Check if there is an associated global index to read cell boundaries
    SpatioTemporalGlobalIndex<SpatioTemporalPartition> gindex = SpatialSite.getSpatioTemporalGlobalIndex(fs, path.getParent());
    if (gindex == null) {
      cellMBR = new SpatioTemporalPartition();
//      cellMBR.invalidate();
    } else {
      // Set from the associated partition in the global index
      for (SpatioTemporalPartition p : gindex) {
        if (p.filename.equals(this.path.getName()))
          cellMBR = p;
      }
    }
  }
  
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (offsetOfNextTree > 0) {
      if (codec == null) {
        // Input is not compressed. Just seek to the next RTree
        filePosition.seek(offsetOfNextTree);
      } else {
        // Input is compressed. We must have read the whole R-tree already
      }
    }
    if (getPos() >= end)
      return false;
    RTree<V> rtree = new RTree<V>();
    rtree.setStockObject(stockShape);
    rtree.readFields(in);
    this.offsetOfNextTree = rtree.getEndOffset();

    if (inputQueryRange != null) {
      // Apply a query query
      value = rtree.search(inputQueryRange);
      return value.iterator().hasNext();
    } else {
      // Return the tree
      value = rtree;
      return rtree.getElementCount() > 0;
    }
  }
  
  public long getPos() throws IOException {
    return filePosition.getPos();
  }

  @Override
  public Partition getCurrentKey() throws IOException, InterruptedException {
    return cellMBR;
  }
  
  public static class DuplicateAvoidanceIterator<V extends StShape> implements Iterable<V>, Iterator<V> {
    /**MBR of the containing cell to run the reference point technique*/
    private Interval cellMBR;
    /**MBR of the query range*/
    private Interval inputQueryMBR;
    /**All underlying values*/
    private Iterator<V> values;
    /**The value that will be returned next*/
    private V nextValue;

    public DuplicateAvoidanceIterator(Interval cellMBR,
        Interval inputQueryMBR, Iterator<V> values) {
      this.cellMBR = cellMBR;
      this.inputQueryMBR = inputQueryMBR;
      this.values = values;
      getNextValue();
    }
    
    public boolean isMatched(StShape shape) {
      // Apply reference point duplicate avoidance technique
      Interval shapeMBR = shape.getMBR();
      long reference_t1 = Math.min(inputQueryMBR.t1, shapeMBR.t1);
      long reference_t2 = Math.max(inputQueryMBR.t2, shapeMBR.t2);
      return cellMBR.contains(reference_t1, reference_t2);
    }
    
    @Override
    public Iterator<V> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return nextValue != null;
    }

    @Override
    public V next() {
      V currentValue = (V) nextValue.clone();
      getNextValue();
      return currentValue;
    }

    private void getNextValue() {
      do {
        nextValue = values.next();
      } while (values.hasNext() && !isMatched(nextValue));
      if (nextValue == null || !isMatched(nextValue))
        nextValue = null;
    }
    
    @Override
    public void remove() {
      throw new RuntimeException("Non-implemented method");
    }
    
  }

  @Override
  public Iterable<V> getCurrentValue() throws IOException, InterruptedException {
    if (cellMBR.isValid() && inputQueryMBR != null) {
      // need to run a duplicate avoidance technique on all results
      return new DuplicateAvoidanceIterator<V>(cellMBR, inputQueryMBR, value.iterator());
    }
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f,
        (getPos() - start) / (float)(end - start));
    }
  }

  @Override
  public void close() throws IOException {
    try {
      in.close();
      in = null;
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
      }
    }

  }

}
