import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static junit.framework.TestCase.assertFalse;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Created by ggchangan on 17-6-22.
 */
public class ShowFileStatusTest {
    private MiniDFSCluster cluster;
    private FileSystem fs;

    @Before
    public void setUp() throws IOException {
        Configuration conf = new Configuration();

        cluster = new MiniDFSCluster(conf, 1, true, null);
        fs = cluster.getFileSystem();

        OutputStream out = fs.create(new Path("/dir/file"));
        out.write("content".getBytes(StandardCharsets.UTF_8));
        out.close();
    }

    @After
    public void tearDown() throws IOException {
        if (fs != null) {
            fs.close();
        }

        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void throwsFileNotFoundForNonExistentFile() throws IOException {
        fs.getStatus(new Path("no-such-fle"));
    }

    @Test
    public void fileStatusForFile() throws IOException {
        Path file = new Path("/dir/file");
        FileStatus stat = fs.getFileStatus(file);
        assertEquals("/dir/file", stat.getPath().toUri().getPath());
        assertFalse(stat.isDirectory());
        assertThat(stat.getLen()).isEqualTo(7);
        assertThat(stat.getModificationTime()).isLessThan(System.currentTimeMillis());
        assertThat(stat.getReplication()).isEqualTo((short) 1);
        assertThat(stat.getBlockSize()).isEqualTo(128 * 1024 * 1024);
        assertThat(stat.getOwner()).isEqualToIgnoringCase("magneto");
        assertThat(stat.getGroup()).isEqualToIgnoringCase("supergroup");
        assertThat(stat.getPermission().toString()).isEqualToIgnoringCase("rw-r--r--");
    }

    @Test
    public void fileStatusForDirectory() throws IOException {
        Path dir = new Path("/dir");
        FileStatus stat = fs.getFileStatus(dir);
        assertThat(stat.getPath().toUri().getPath()).isEqualToIgnoringCase("/dir");
        assertThat(stat.isDirectory()).isTrue();

        assertThat(stat.getLen()).isEqualTo(0L);
        assertThat(stat.getModificationTime()).isLessThanOrEqualTo(System.currentTimeMillis());
        assertThat(stat.getReplication()).isEqualTo((short)0);
        assertThat(stat.getBlockSize()).isEqualTo(0L);
        assertThat(stat.getOwner()).isEqualToIgnoringCase("magneto");
        assertThat(stat.getGroup()).isEqualToIgnoringCase("supergroup");
        assertThat(stat.getPermission().toString()).isEqualToIgnoringCase("rwxr-xr-x");
    }
}
