package mr.matrix;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * mapreduce 矩阵乘法
 * 测试矩阵如下：
 * A = （aij)
 *      1 2 3
 *      4 5 0
 *      7 8 9
 *      10 11 12
 * B =  (bjk)
 *      10 15
 *      0  2
 *      11 9
 */
public class MatrixDriver extends Configured implements Tool{
    public final static Integer M = 4;
    public final static Integer N = 3;
    public final static Integer L = 2;
    public final static String SEPARATOR = ",";
    public final static String MATRIX_A = "A";
    public final static String MATRIX_B = "B";

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("Matrix Multiplication");
        job.setJarByClass(MatrixDriver.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        /*
        String resource = "/matrix";
        System.out.println(getClass().getResource(resource).getPath());
        File[] files = new File(getClass().getResource(resource).getPath()).listFiles();
        */
        String resource = "matrix";
        File[] files = new File(resource).listFiles();

        for (File file: files) {
            //System.out.println(file.getAbsolutePath());
            //System.out.println(file.getPath());
            FileInputFormat.addInputPath(job, new Path(file.getPath()));
        }

        Path matrixOutput = new Path("matrixOutput");
        FileOutputFormat.setOutputPath(job, matrixOutput);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MatrixDriver(), args);
        System.exit(exitCode);
    }
}
