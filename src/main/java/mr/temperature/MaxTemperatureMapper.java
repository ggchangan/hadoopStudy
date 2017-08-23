package mr.temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MaxTemperatureMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        parser.parse(value);

        if (parser.isValidTemperature()) {
            int airTemperature = parser.getAirTemperature();
            context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
        } else if (parser.isMalformedTemperature()) {
            System.err.println("Ignoring possibly corrupt input:" + value);
            context.getCounter(Temperature.MALFORMED).increment(1);
        } else if (parser.isMissingTemperature()) {
            context.getCounter(Temperature.MISSING).increment(1);
        }

        context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
    }

    enum Temperature {
        MISSING,
        MALFORMED
    }
}
