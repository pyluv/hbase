package com.learning.hdfsImportMR;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;


import java.io.IOException;

/**
 * @Classname HbaseReducer
 * @Description TODO
 * @Date 4/28/2020 10:00 AM
 * @Created by Administrator
 */
public class HbaseReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        //读出来的每一行数据写入到 fruit_hdfs 表中
        for (Put put : values) {
            context.write(NullWritable.get(), put);
        }
    }
}
