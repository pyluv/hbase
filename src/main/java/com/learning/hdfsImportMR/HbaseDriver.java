package com.learning.hdfsImportMR;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Classname HbaseDriver
 * @Description TODO
 * @Date 4/28/2020 10:00 AM
 * @Created by Administrator
 */
public class HbaseDriver implements Tool {
    //定义一个Configuration
    private Configuration configuration = null;

    public int run(String[] args) throws Exception {
        //得到 Configuration
        Configuration conf = this.getConf();

        //创建 Job 任务
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(HbaseDriver.class);
        Path inPath = new Path("hdfs://hadoop102:9000/fruit.tsv");
        FileInputFormat.addInputPath(job, inPath);

        //设置 Mapper
        job.setMapperClass(HbaseMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置 Reducer
        TableMapReduceUtil.initTableReducerJob("fruit1", HbaseReducer.class, job);

        //设置 Reduce 数量，最少 1 个
        job.setNumReduceTasks(1);

        boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess) {
            throw new IOException("Job running with error");
        }

        return isSuccess ? 0 : 1;
    }

    public void setConf(Configuration conf) {
        configuration = conf;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {

        Configuration conf = HBaseConfiguration.create();

        int status = 0;
        try {
            status = ToolRunner.run(conf, new HbaseDriver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.exit(status);
    }
}
