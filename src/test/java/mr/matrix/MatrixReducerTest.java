package mr.matrix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class MatrixReducerTest {

    @Test
    public void reduce() throws Exception {
        new ReduceDriver<Text, Text, Text, IntWritable>()
                .withReducer(new MatrixReducer())
                .withInput(new Text("1,1"), Arrays.asList(
                        new Text("A,1,1"),
                        new Text("A,2,2"),
                        new Text("A,3,3"),
                        new Text("B,1,10"),
                        new Text("B,3,11")
                ))
                .withOutput(new Text("1,1"), new IntWritable(43))
                .runTest();
    }

}