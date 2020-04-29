package com.learning.tableImportMR;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @Classname ImportTableReducer
 * @Description TODO
 * @Date 4/29/2020 9:17 AM
 * @Created by Administrator
 */
public class WriteTableReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        //读出来的每一行数据写入到 fruit_mr 表中
        for (Put put : values) {
            context.write(NullWritable.get(), put);
        }
    }
}
