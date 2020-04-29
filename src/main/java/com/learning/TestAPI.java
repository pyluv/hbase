package com.learning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Classname TestAPI
 * @Description TODO
 * @Date 4/27/2020 9:09 AM
 * @Created by Administrator
 */
public class TestAPI {
    private static Connection connection = null;
    private static Admin admin = null;
    private static Configuration configuration;

    static {
        //1. 获取配置文件信息
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        try {
            //2.创建连接对象
            connection = ConnectionFactory.createConnection(configuration);
            //3.创建Admin对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //判断表是否存在
    public static boolean isTableExist(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }


    //創建表
    public static void createTable(String tableName, String... columnFamilies) throws IOException {
        //1.判断是否存在列族信息
        if (columnFamilies.length <= 0) {
            System.out.println("请设置列族信息");
            return;
        }
        //2.判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表已存在");
            return;
        }
        //3.创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //4.循环添加列族
        for (String cf : columnFamilies) {
            //5.创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

            //6.添加具体的列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        //7.创建表
        admin.createTable(hTableDescriptor);
    }

    //刪除表
    public static void dropTable(String tableName) throws IOException {
        //1.判斷表是否不存在
        if (!isTableExist(tableName)) {
            System.out.println(tableName + "表不存在");
            return;
        }
        //2.使表下线
        admin.disableTable(TableName.valueOf(tableName));
        //3.删除表
        admin.deleteTable(TableName.valueOf(tableName));

    }

    public static void createNameSpace(String ns) {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println(ns + "命名空间已存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void putData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        //创建 Table 对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向 Put 对象中组装数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        //插入数据
        table.put(put);
        //关闭资源
        table.close();
    }

    public static void getData(String tableName, String rowKey) throws IOException {
        getData(tableName, rowKey, "");
    }

    public static void getData(String tableName, String rowKey, String columnFamily) throws IOException {
        getData(tableName, rowKey, columnFamily, "");
    }

    public static void getData(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        //创建 Table 对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建 Get 对象
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!columnFamily.equals("")) {
            get.addFamily(Bytes.toBytes(columnFamily));
        }

        if (!column.equals("")) {
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        }

        //获取数据
        Result result = table.get(get);
        //解析result并打印
        for (Cell cell : result.rawCells()) {
            System.out.println("ColumnFamily :" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("Column :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("Value :" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
    }

    public static void scanData(String tableName) throws IOException {
        //创建 Table 对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RowKey: " + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("ColumnFamily :" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("Column :" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("Value :" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }

    public static void deleteData(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        //创建 Table 对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
    }


    public static void main(String[] args) throws IOException {
        //1.测试表是否存在
        System.out.println(isTableExist("stu"));

        //2.创建表测试
        //createTable("stu","info1", "info2");

        //1.测试表是否存在
        //System.out.println(isTableExist("stu"));

        //dropTable("stu");
        //System.out.println(isTableExist("stu"));
        //putData("stu","1001","info1", "sex", "male");
//        getData("stu", "1001");
//        scanData("stu");
//        deleteData("stu", "1001","","");
        scanData("stu");

        //关闭资源
        close();

    }
}
