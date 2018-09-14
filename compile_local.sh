#!/bin/bash
mvn compile
mvn assembly:assembly
mv ./target/sthadoop-2.4.1-SNAPSHOT.jar ./target/st-hadoop-uber.jar
scp ./target/st-hadoop-uber.jar alar0021@cs-spatial-210:/export/scratch/louai/idea-stHadoop/
scp ./target/sthadoop-2.4.1-SNAPSHOT-sources.jar alar0021@cs-spatial-210:/export/scratch/louai/idea-stHadoop/
