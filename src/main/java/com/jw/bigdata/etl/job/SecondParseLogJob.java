package com.jw.bigdata.etl.job;

import com.jw.bigdata.etl.mr.LogBeanWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class SecondParseLogJob extends Configured implements Tool {

    /**
     *   解析日志方法
     * @param row
     * @return
     * @throws ParseException
     */
    public static LogBeanWritable parseLong(String row) throws ParseException {
        String[] logPart = StringUtils.split(row, "$$");

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long timeTag = dateFormat.parse(logPart[0]).getTime();
        String logFileName = logPart[1];
        String logLevel = logPart[2];
        String serviceName = logPart[3];
        String threadName = logPart[4];
        String className = logPart[7];

        LogBeanWritable logBeanWritable = new LogBeanWritable();
        logBeanWritable.setClassName(className);
        logBeanWritable.setLogFileName(logFileName);
        logBeanWritable.setServiceName(serviceName);
        logBeanWritable.setThreadName(threadName);
        logBeanWritable.setLogLevel(logLevel);
        logBeanWritable.setTimeTag(timeTag);
        return logBeanWritable;
    }

    /**
     *  Mapper
     */
    public static class LogMapper extends Mapper<LongWritable, Text, LongWritable, LogBeanWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                LogBeanWritable logBeanWritable = parseLong(value.toString());
                context.write(key, logBeanWritable);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *  Reducer
     */
    public static class LogRuducer extends Reducer<LongWritable, LogBeanWritable, NullWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<LogBeanWritable> values, Context context) throws IOException, InterruptedException {
            for (LogBeanWritable value : values) {
                context.write(null, new Text(value.toString()));
            }
        }
    }

    /**
     *  封装 MR Task
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SecondParseLogJob.class);
        job.setJobName("SecondParseLogJob");
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogRuducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LogBeanWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        if (!job.waitForCompletion(true)) {
            throw new RuntimeException(job.getJobName() + "   failed !");
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new SecondParseLogJob(), args);
        System.exit(run);
    }


}
