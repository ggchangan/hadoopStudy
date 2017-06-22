import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ggchangan on 17-6-22.
 * 写操作，默认是覆盖操作
 */
public class FileCopyWithProgress {

    public static void main(String[] args) throws IOException {
        String dst = "hdfs://lc14/user/ggchangan/output";


        Configuration conf = new Configuration();
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "never");
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        Path path = new Path(dst);
        Progressable progressable = new Progressable() {
            @Override
            public void progress() {
                System.out.println(".");
            }
        };

        OutputStream out = null;
        if (!fs.exists(path.getParent())) {
            System.out.println("start to writer ... ");
            out = fs.create(path, progressable);

            String localStr = "input";
            InputStream in = new BufferedInputStream(new FileInputStream(localStr));
            IOUtils.copyBytes(in, out, 4096, true);
        } else {
            System.out.println("start to append ...");
            out = fs.append(path, 4096, progressable);
            String line = "2 test append\n";
            out.write(line.getBytes());
        }
    }
}
