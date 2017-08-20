package mr.temperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.*;

public class MaxTemperatureDriverTest {

    @Test
    public void testDriver() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");

        Path input = new Path("ncdc/micro/sample.txt");
        Path output = new Path("output");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true);

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);

        int exitCode = driver.run(new String[]{
                input.toString(),
                output.toString()
        });

        assertThat(exitCode, is(0));

        checkOutput(conf, output);
    }

    private void checkOutput(Configuration conf, Path output) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        Path[] outputFiles = FileUtil.stat2Paths(
                fs.listStatus(output, new OutputLogFilter()));

        //_SUCCESS & part-r-00000
        assertThat(outputFiles.length, is(2));

        Path outputPath = outputFiles[0].getName().equalsIgnoreCase("_SUCCESS")?
                outputFiles[1] : outputFiles[0];

        assertThat(outputPath.getName(), is("part-r-00000"));

        BufferedReader actual = asBufferedReader(fs.open(outputPath));
        BufferedReader expected = asBufferedReader(
                getClass().getResourceAsStream("/expected.txt"));

        String expectedLine;
        while ((expectedLine = expected.readLine()) != null) {
            assertThat(actual.readLine(), is(expectedLine));
        }
        assertThat(actual.readLine(), nullValue());
        actual.close();
        expected.close();
    }

    private BufferedReader asBufferedReader(InputStream in) throws IOException {
        return new BufferedReader(new InputStreamReader(in));
    }

}

