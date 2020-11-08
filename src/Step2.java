import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 第二步的job，产生统计文件，内容为：
 * 单词   类别  次数
 */
public class Step2 {

    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outVal = new Text();

        /**
         * inputKey: offset
         * inputVal: className split1 split2...
         * output: <word:className, 1>
         *
         * @param key     偏移量
         * @param value   一篇文章的所属class+splits
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //分隔
            StringTokenizer tokens = new StringTokenizer(value.toString(), "\t\n\r\f, ");
            boolean isFirst = true;
            String className = null;
            while (tokens.hasMoreTokens()) {
                String token = tokens.nextToken();
                if (isFirst) {
                    //首行开头为类别
                    className = token;
                    isFirst = false;
                    continue;
                }
                try {
                    //<word:class, 1>表示key值出现1次
                    outKey.set(token + ":" + className);
                    outVal.set("1");
                    context.write(outKey, outVal);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * input:
         * <word:className, list(1,1,1,...)>
         * output:
         * <word, className+'\t'+sum>
         *
         * @param key     className:word
         * @param values  list(1,1,...)
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String keyStr = key.toString();
            int pos = keyStr.indexOf(":");
            String word = keyStr.substring(0, pos);
            String className = keyStr.substring(pos + 1);
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            Text outputKey = new Text(word);
            Text outputVal = new Text(className + "\t" + sum);
            context.write(outputKey, outputVal);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step2");
        job.setJarByClass(Step2.class);
        //设置mapper，reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //设置reduce的数量
//        job.setNumReduceTasks(3);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
}
