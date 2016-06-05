mvn compile
mvn assembly:assembly
mv ./target/spatialhadoop-2.4.1-SNAPSHOT-uber.jar st-hadoop-uber.jar
scp ./target/st-hadoop-uber.jar louai@cs-spatial-210:/export/scratch/louai/idea-stHadoop/
