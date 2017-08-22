package mr.matrix;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MatrixMapperTest {

    @Test
    public void map() throws Exception {
        Text text = new Text("1 3 2");

        List<Pair<Text, Text>> output = new ArrayList<>();
        for (int i=1; i<=MatrixDriver.L; i++) {
            output.add(new Pair<>(new Text("1"+MatrixDriver.SEPARATOR+i),
                    new Text(MatrixDriver.MATRIX_A + MatrixDriver.SEPARATOR
                            + "3" + MatrixDriver.SEPARATOR
                            + "2"
                    )));
        }

        //测试MatrixA
        new MapDriver<LongWritable, Text, Text, Text>()
                .withMapper(new MatrixMapper())
                .withMapInputPath(new Path("m1.csv"))
                .withInput(new LongWritable(0), text)
                .withAllOutput(output)
                .runTest();

        output = new ArrayList<>();
        for (int i=1; i<=MatrixDriver.M; i++) {
            output.add(new Pair<>(new Text(i+MatrixDriver.SEPARATOR+3),
                    new Text(MatrixDriver.MATRIX_B + MatrixDriver.SEPARATOR
                            + "1" + MatrixDriver.SEPARATOR
                            + "2"
                    )));
        }

        //测试MatrixB
        new MapDriver<LongWritable, Text, Text, Text>()
                .withMapper(new MatrixMapper())
                .withMapInputPath(new Path("m2.csv"))
                .withInput(new LongWritable(0), text)
                .withAllOutput(output)
                .runTest();
    }

}