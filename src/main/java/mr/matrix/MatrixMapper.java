package mr.matrix;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MatrixMapper extends Mapper<LongWritable, Text, Text, Text>{

    @Override
    protected void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
        String line = text.toString();

        if (line == null || line.isEmpty()) return;

        String[] values = line.split(" ");

        if (values.length < 3) return;

        String i = values[0];
        String j = values[1];
        String value = values[2];

        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        if (fileName.contains("m1")) {
            for (int k = 1; k <= MatrixDriver.L; k ++) {
                Text outKey = new Text(i + MatrixDriver.SEPARATOR + k);
                Text outValue = new Text(MatrixDriver.MATRIX_A + MatrixDriver.SEPARATOR + j
                        + MatrixDriver.SEPARATOR  + value);

                context.write(outKey, outValue);
            }
        }

        if (fileName.contains("m2")) {
            for (int k = 1; k <= MatrixDriver.M; k ++) {
                Text outKey = new Text(k + MatrixDriver.SEPARATOR + j);
                Text outValue = new Text(MatrixDriver.MATRIX_B + MatrixDriver.SEPARATOR + i
                        + MatrixDriver.SEPARATOR  + value);

                context.write(outKey, outValue);
            }

        }


    }
}
