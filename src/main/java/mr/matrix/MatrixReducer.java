package mr.matrix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> row = new HashMap<>();
        Map<String, Integer> col = new HashMap<>();


        for (Text value: values) {
            String[] splitValues = value.toString().split(MatrixDriver.SEPARATOR);
            String j = splitValues[1];
            Integer ele = Integer.valueOf(splitValues[2]);

            if (splitValues[0].equalsIgnoreCase(MatrixDriver.MATRIX_A)) {
                row.put(j, ele);
            } else {
                col.put(j, ele);
            }
        }

        Integer outputEle = row.entrySet().stream().map(entry ->
                col.containsKey(entry.getKey()) ? entry.getValue() * col.get(entry.getKey()) : 0
        ).reduce(0, (a, b) -> a + b);

        context.write(key, new IntWritable(outputEle));

    }
}
