echo "Text" | hadoop jar build/libs/hadoopStudy.jar hdfs.compress.StreamCompressor org.apache.hadoop.io.compress.GzipCodec | gunzip
echo "张忍" | hadoop jar build/libs/hadoopStudy.jar hdfs.compress.StreamCompressor org.apache.hadoop.io.compress.GzipCodec | gunzip
