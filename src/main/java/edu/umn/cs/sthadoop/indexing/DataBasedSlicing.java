package edu.umn.cs.sthadoop.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.ResultCollector;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.indexing.Indexer;
import edu.umn.cs.spatialHadoop.io.TextSerializable;
import edu.umn.cs.spatialHadoop.operations.Sampler;
import edu.umn.cs.spatialHadoop.util.FileUtil;
import edu.umn.cs.sthadoop.core.STPoint;

public class DataBasedSlicing {
	private static final Log LOG = LogFactory.getLog(Indexer.class);
/**
 * Create a partitioner for a particular job
 * @param ins
 * @param out
 * @param paramss
 * @throws IOException
 */
	static void slicing(Path[] ins, Path out,
			  OperationsParams paramss) throws IOException {
		  
		JobConf conf = new JobConf(paramss, STSampler.class);
//		  job.setJarByClass(TimeBasedSlicing.class);
		  
		  final List<Point> sample = new ArrayList<Point>();
	      float sample_ratio = conf.getFloat(SpatialSite.SAMPLE_RATIO, 0.01f);
	      long sample_size = conf.getLong(SpatialSite.SAMPLE_SIZE, 100 * 1024 * 1024);
	      
	      
	      
	   // Determine number of partitions
	      long inSize = 0;
	      for (Path in : ins) {
	        inSize += FileUtil.getPathSize(in.getFileSystem(conf), in);
	      }
	      long estimatedOutSize = (long) (inSize * (1.0 + conf.getFloat(SpatialSite.INDEXING_OVERHEAD, 0.1f)));
	      FileSystem outFS = out.getFileSystem(conf);
	      long outBlockSize = outFS.getDefaultBlockSize(out);
	      
	      LOG.info("Reading a sample of "+(int)Math.round(sample_ratio*100) + "%");
	      ResultCollector<TextSerializable> output =
				    new ResultCollector<TextSerializable>() {
				      @Override
				      public void collect(TextSerializable value) {
				        System.out.println(value.toText(new Text()));
				      }
				    };

	      
		  paramss.setFloat("ratio", sample_ratio);
		  paramss.setLong("size", sample_size);
	      long t1 = System.currentTimeMillis();
	      paramss.set("shape", conf.get("shape"));
	      if (conf.get("local") != null)
	    	  paramss.set("local", conf.get("local"));
	      paramss.setClass("outshape", STPoint.class, Shape.class);
	      STSampler.sample(ins, output, paramss);
	      long t2 = System.currentTimeMillis();
	      System.out.println("Total time for sampling in millis: "+(t2-t1));
	      LOG.info("Finished reading a sample of "+sample.size()+" records");
	      
	      int partitionCapacity = (int) Math.max(1, Math.floor((double)sample.size() * outBlockSize / estimatedOutSize));
	      int numPartitions = Math.max(1, (int) Math.ceil((float)estimatedOutSize / outBlockSize));
	      LOG.info("Partitioning the space into "+numPartitions+" partitions with capacity of "+partitionCapacity);
	      
		  
		 

				    
		
	  }
	  
	  protected static void printUsage() {
		    System.out.println("Builds a spatial index on an input file");
		    System.out.println("Parameters (* marks required parameters):");
		    System.out.println("<input file> - (*) Path to input file");
		    System.out.println("<output file> - (*) Path to output file");
		    System.out.println("shape:<point|rectangle|polygon> - (*) Type of shapes stored in input file");
		    System.out.println("sindex:<index> - (*) Type of spatial index (grid|str|str+|quadtree|zcurve|kdtree)");
		    System.out.println("-overwrite - Overwrite output file without noitce");
		    GenericOptionsParser.printGenericCommandUsage(System.out);
		  }

		  /**
		   * Entry point to the indexing operation.
		   * @param args
		   * @throws Exception
		   */
		  public static void main(String[] args) throws Exception {
			args = new String[5];
		    args[0] = "/export/scratch/louai/scratch1/workspace/dataset/sthadoop/input";
		    args[1] = "/export/scratch/louai/scratch1/workspace/dataset/sthadoop/output";
		    args[2] = "sindex:grid";
		    args[3] = "shape:edu.umn.cs.spatialHadoop.core.TemporalTweets";
		    args[4] = "-overwrite";
//			  
		    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
		    
		    if (!params.checkInputOutput(true)) {
		      printUsage();
		      return;
		    }
		    if (params.get("sindex") == null) {
		      System.err.println("Please specify type of index to build (grid, rtree, r+tree, str, str+)");
		      printUsage();
		      return;
		    }
		    Path[] inputPath = params.getInputPaths();
		    Path outputPath = params.getOutputPath();

		    // The spatial index to use
		    long t1 = System.currentTimeMillis();
		    slicing(inputPath, outputPath, params);
		    long t2 = System.currentTimeMillis();
		    System.out.println("Total Slicing time in millis "+(t2-t1));
		  }

}
