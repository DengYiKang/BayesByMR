import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

public class tmp {
    public static void main(String[] args) throws URISyntaxException, IOException {
        String uri = "hdfs://master:9000";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(uri), conf);
        Path path = new Path("/user/root/test");
        FileStatus[] status = fs.listStatus(path);
        Path[] listedPaths = FileUtil.stat2Paths(status);
        for (Path p : listedPaths) {
            InputStream in = fs.open(p);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            reader.close();
            in.close();
        }
    }
}
