package com.jw.bigdata.etl.job;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.text.SimpleDateFormat;

public class ParseLogJob extends Configured implements Tool {

    /**
     * 解析日志方法
     *
     * @param row
     * @return
     * @throws Exception
     */
    public static Text parseLog(String row) throws Exception {
        String[] logPart = StringUtils.split(row, "$$");
        System.out.println("=======================================");
        for (String s : logPart) {
            System.out.println(s);
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long timeTag = dateFormat.parse(logPart[0]).getTime();
        String logFileName = logPart[1];
        String logLevel = logPart[2];
        String serviceName = logPart[3];
        String threadName = logPart[4];
        String className = logPart[7];

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("timeTag", timeTag);
        jsonObject.put("logFileName", logFileName);
        jsonObject.put("logLevel", logLevel);
        jsonObject.put("serviceName", serviceName);
        jsonObject.put("threadName", threadName);
        jsonObject.put("className", className);
        return new Text(jsonObject.toJSONString());
    }


    /**
     * mapper
     */
    public static class LogMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)  {
            try {
                Text text = parseLog(value.toString());
                context.write(null, text);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ParseLogJob.class);
        job.setJobName("parseJob");
        job.setMapperClass(LogMapper.class);
        job.setNumReduceTasks(0);

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
        int run = ToolRunner.run(new Configuration(), new ParseLogJob(), args);
        System.exit(run);
    }
}
