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
 * 第一步的job，产生统计文件，内容为：
 * 类别c   类别为c的文档个数     类别为c的文档出现的单词的总数  类别为c的文档出现的单词的种数
 */
public class Step1 {

    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outVal = new Text();

        /**
         * inputKey: offset
         * inputVal: className split1 split2...
         * output1: <className, 1>
         * output2: <className:word, 1>
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
                    //<class:1>表示该类的实例出现了一次
                    outKey.set(className);
                    outVal.set("1");
                    context.write(outKey, outVal);
                    continue;
                }
                try {
                    //<class:word, 1>表示key值出现1次
                    outKey.set(className + ":" + token);
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

    public static class MyCombiner extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outVal = new Text();


        /**
         * input:
         * <className, 1> or <className:word, 1>
         * output:
         * <className, list(1,1,...)> => <className, sum1>
         * <className:word, list(1,1,...)> => <className, word:sum2>
         *
         * @param key     className or className:word
         * @param values  count
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            String keyStr = key.toString();
            int pos = keyStr.indexOf(":");
            if (pos == -1) {
                outKey.set(key);
                outVal.set(String.valueOf(sum));
            } else {
                outKey.set(keyStr.substring(0, pos));
                outVal.set(keyStr.substring(pos + 1) + ":" + sum);
            }
            context.write(outKey, outVal);
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * input:
         * <className, list(sum1, word:sum2, word:sum2, ...>
         * output:
         * <className, num1 num2 num3>
         * 其中，num1表示类别为className的文档个数，num2表示类别className的所有文档出现的单词的总数，
         * num3表示类别className的所有文档出现的单词的种数
         *
         * @param key     className
         * @param values  list(sum1, word:sum2, word:sum2, ...)
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int num1 = 0, num2 = 0, num3 = 0;
            for (Text value : values) {
                String str = value.toString();
                int pos = str.indexOf(":");
                if (pos == -1) {
                    num1 += Integer.parseInt(str);
                } else {
                    num2 += Integer.parseInt(str.substring(pos + 1));
                    num3++;
                }
            }
            context.write(key, new Text(num1 + "\t" + num2 + "\t" + num3));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step1");
        job.setJarByClass(Step1.class);
        //设置mapper，combiner，reducer
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
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
