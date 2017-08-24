package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ListStatus {

    public static void main(String[] args) throws IOException {
        String uri = args[0];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        List<Path> paths = Arrays.stream(args).map(arg -> new Path(arg)).collect(Collectors.toList());

        FileStatus[] status = fs.listStatus(paths.toArray(new Path[paths.size()]));
        Arrays.stream(FileUtil.stat2Paths(status)).forEach(System.out::println);
    }
}
