package com.jw.bigdata.etl.mr;

import lombok.Data;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


@Data
public class LogBeanWritable implements Writable {


    private String logLevel;
    private String logFileName;
    private String className;
    private String serviceName;
    private String threadName;
    private long timeTag;


    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, logLevel);
        WritableUtils.writeString(out, logFileName);
        WritableUtils.writeString(out, className);
        WritableUtils.writeString(out, serviceName);
        WritableUtils.writeString(out, threadName);
        out.writeLong(timeTag);
    }

    public void readFields(DataInput in) throws IOException {
        logLevel = WritableUtils.readString(in);
        logFileName = WritableUtils.readString(in);
        className = WritableUtils.readString(in);
        serviceName = WritableUtils.readString(in);
        threadName = WritableUtils.readString(in);
        timeTag = in.readLong();
    }

}
