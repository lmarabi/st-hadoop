#!/bin/bash

echo 'compile and update on github server'
echo 'please enter commit message'
read txt
echo you entered "$txt"
git pull
git add --al .
git commit -m "$txt"
git push 
mvn compile
mvn assembly:assembly
mv ./target/sthadoop-2.4.1-SNAPSHOT-uber.jar st-hadoop-uber.jar
scp st-hadoop-uber.jar louai@cs-spatial-210:/export/scratch/louai/idea-stHadoop/
cp st-hadoop-uber.jar /export/scratch/louai/scratch1/hadoopDir/localCluster/hadoop-2.7.2/bin/
#cp st-hadoop-uber.jar /export/scratch/louai/scratch1/hadoopDir/localCluster/hadoop-2.7.2/bin/
