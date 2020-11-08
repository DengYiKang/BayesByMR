import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * 第三步的job
 */
public class Step3 {

    //term:class
    private static Hashtable<String, Integer> termsTable = new Hashtable<>();
    //N:class; T:class; V:class
    private static Hashtable<String, Integer> classesTable = new Hashtable<>();

    private static final String URI = "hdfs://master:9000";

    private static List<String> classList = new ArrayList<>();

    /**
     * 计算P(curClass|content)的相对大小
     *
     * @param content  文章内容
     * @param curClass 分类名
     * @return
     */
    private static double calClass(String content, String curClass) {
        String[] words = content.trim().split("\t| ");
        //N表示文档总数
        double N = 0;
        for (String className : classList) {
            N += classesTable.get("N:" + className);
        }
        double curProb = 0;
        double p_c = Math.log(classesTable.get("N:" + curClass) / N);
//        double p_c = classesTable.get("N:" + curClass) / N;
        double p_term_c = 0;
//        double p_term_c = 1;
        for (String word : words) {
            double tmp = ((double) termsTable.getOrDefault(word + ":" + curClass, 0) + 1)
                    / ((double) classesTable.get("T:" + curClass) + (double) classesTable.get("V:" + curClass));
            p_term_c += Math.log(tmp);
//            p_term_c *= tmp;
        }
        curProb = p_c + p_term_c;
//        curProb = p_c * p_term_c;
        return curProb;
    }

    private static void buildClassesTable() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(URI), conf);
        Path path = new Path("/user/root/step1");
        FileStatus[] status = fs.listStatus(path);
        Path[] paths = FileUtil.stat2Paths(status);
        for (Path p : paths) {
            InputStream in = fs.open(p);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line = reader.readLine()) != null) {
                String[] splits = line.trim().split("\t| ");
                String className = splits[0];
                String N = splits[1];
                String T = splits[2];
                String V = splits[3];
                classesTable.put("N:" + className, Integer.parseInt(N));
                classesTable.put("T:" + className, Integer.parseInt(T));
                classesTable.put("V:" + className, Integer.parseInt(V));
                classList.add(className);
            }
        }
    }

    private static void buildTermsTable() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(URI), conf);
        Path path = new Path("/user/root/step2");
        FileStatus[] status = fs.listStatus(path);
        Path[] paths = FileUtil.stat2Paths(status);
        for (Path p : paths) {
            InputStream in = fs.open(p);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line = "";
            while ((line = reader.readLine()) != null) {
                String[] splits = line.trim().split("\t| ");
                String term = splits[0];
                String className = splits[1];
                String count = splits[2];
                termsTable.put(term + ":" + className, Integer.parseInt(count));
            }
        }
    }

    public static class MyMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                buildClassesTable();
                buildTermsTable();
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }

        /**
         * input format
         * fileIndex content
         * output:
         * <fileIndex, className:prob>
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().trim().split("\t| ", 2);
            String fileIndex = splits[0];
            String content = splits[1];
            for (String className : classList) {
                System.out.println(className + " " + calClass(content, className));
                context.write(new Text(fileIndex), new Text(className + ":" + calClass(content, className)));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {


        /**
         * input:
         * <fileIndex, list(className:prob)>
         * output:
         * <fileIndex, className>
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //定义maxProb为Double.MIN_VALUE，后续的比较会出bug
            double maxProb = 0;
            String ansClass = "";
            boolean isFirst = true;
            for (Text value : values) {
                String str = value.toString();
                int pos = str.indexOf(":");
                String className = str.substring(0, pos);
                double prob = Double.parseDouble(str.substring(pos + 1));
                if (isFirst) {
                    isFirst = false;
                    maxProb = prob;
                    ansClass = className;
                } else if (prob > maxProb) {
                    maxProb = prob;
                    ansClass = className;
                }
            }
            context.write(key, new Text(ansClass));
//            context.write(key, new Text(String.valueOf(maxProb)));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //设置reduce的数量
//        job.setNumReduceTasks(3);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
}
