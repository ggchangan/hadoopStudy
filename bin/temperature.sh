#export HADOOP_CLASSPATH=build/libs/hadoopStudy.jar
#hadoop jar build/libs/hadoopStudy.jar mr.temperature.MaxTemperature ncdc/sample.txt output
hadoop jar build/libs/hadoopStudy.jar mr.temperature.MaxTemperatureDriver ncdc/sample.txt output
