ST-Hadoop
=============

[ST-Hadoop](http://st-hadoop.cs.umn.edu) is an extension to SpatialHadoop that provides efficient processing of Spatio-temporal data using MapReduce Hadoop framework. It provides an extendable spatio-temporal data types to be used in MapReduce jobs including STpoint and Interval. ST-Hadoop employs hierarchical multi-resolution spatio-temporal index structure within Hadoop Distributed File System (HDFS) as a mean of efficient retrieval of spatio-temporal data. The fact that time and space are crucial to the query processing influences our design to use two-level indexing of temporal and then spatial as a second level indexing. It also adds a lower-level of spatial indices in HDFS such as Grid file, R-tree, R+-tree, Quad-tree, and other. Some new InputFormats and RecordReaders are also provided to allow reading and processing spatio-temporal indexes efficiently in MapReduce jobs. ST-Hadoop shipped with two basic spatio-temporal operations that are implemented as efficient MapReduce jobs (i.e., **Range query** and **join**) which access ST-Hadoop spatio-temporal index. Developers can implement various spatio-temporal operations that run efficiently using ST-Hadoop data types, operations, and indexes.


How it works
============

ST-Hadoop is used in the same way as Hadoop. Data files are first loaded
into HDFS and indexed using the *stmanager* command which builds the  hierarchical spatio-temporal index structure of your choice of a specified resolution and lower level of a spatial index over the input file. Once the file is indexed, you can execute
any of the spatio-temporal operations provided in ST-Hadoop such as range query and spatio-temporal similarity join. New operations will be added occasionally.


Install
=======

ST-Hadoop is packaged as a single jar file which contains all the required
classes to run. All operations including building the index can be accessed
through this jar file and it gets automatically distributed to all slave nodes
by the Hadoop framework. In addition the *spatial-site.xml* configuration file
needs to be placed in the *conf* directory of your Hadoop installation. This
allows you to configure the cluster accordingly.


Examples
========

Please visit [ST-Hadoop](http://st-hadoop.cs.umn.edu) website there are few examples of how to use ST-Hadoop operations. 

|Operation	|operator 	| Example	|
|:-------------:| :-------------------:| :---------------:|
|spatio-temporal index | stmanager | ./hadoop jar st-hadoop-uber.jar stmanager /geolife_raw_data.txt /index_geolife time:year shape:edu.umn.cs.sthadoop.trajectory.GeolifeTrajectory sindex:str -no-local -overwrite |
|spatio-temporal range query| strangequery | ./hadoop jar st-hadoop-uber.jar strangequery /index_geolife /rq_result shape:edu.umn.cs.sthadoop.trajectory.GeolifeTrajectory rect:39.9111716,116.5956883,39.9119983,116.606835 interval:2008-05-01,2008-05-31 time:year -overwrite |
|Spatio-temporal DTW KNN| dtwknn | ./hadoop jar st-hadoop-uber.jar dtwknn /index_geolife /knndtw_result shape:edu.umn.cs.sthadoop.trajectory.GeolifeTrajectory interval:2008-05-01,2008-05-31 time:year k:20 traj:39.9119983,116.606835x39.9119783,116.6065483x39.9119599,116.6062649x39.9119416,116.6059899x39.9119233,116.6057282x39.9118999,116.6054783x39.9118849,116.6052366x39.9118666,116.6050099x39.91185,116.604775x39.9118299,116.604525x39.9118049,116.6042649x39.91177,116.6040166x39.9117516,116.6037583x39.9117349,116.6035066x39.9117199,116.6032666x39.9117083,116.6030232x39.9117,116.6027566x39.91128,116.5969383x39.9112583,116.5966766x39.9112383,116.5964232x39.9112149,116.5961699x39.9111933,116.5959249x39.9111716,116.5956883 -overwrite -no-local |
    
Compile
=======

Advanced users and contributors might like to compile ST-Hadoop on their own machines.
ST-Hadoop can be compiled via [Maven](http://maven.apache.org/).
First, you need to grab your own version of the source code. You can do this through [git](http://git-scm.com/).
The source code resides on [github](http://github.com). To clone the repository, run the following command

    git clone https://github.com/lmarabi/st-hadoop.git
    
If you do not want to use git, you can still download it as a
[single archive](https://github.com/lmarabi/st-hadoop/archive/master.zip) provided by github.

Once you download the source code, you need to make sure you have any and Ivy installed on your system.
Please check the installation guide of [Maven](http://maven.apache.org/install.html) if you do not have it installed.

To compile ST-Hadoop, navigate to the source code and run the command:

    mvn compile

This will automatically retrieve all dependencies and compile the source code.

To build a redistribution package, run the command:

    mvn assembly:assembly

This Maven command will package all classes of ST-Hadoop along with the dependent jars
not included in Hadoop into an archive. This archive can be used to install ST-Hadoop
on any existing Hadoop cluster.

