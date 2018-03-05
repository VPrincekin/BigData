package HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

public class HBaseDemo {
    public static final String TABLE_NAME = "test_table";
    private static Configuration conf = null;
    //提供了一个接口来管理HBase数据库的表信息。它提供的方法包括：创建表，删除表，列出表项。使表有效或无效，以及添加或删除表列簇成员等。
    private static HBaseAdmin admin = null;
    //获取一个表的实例,可以用来和HBase表直接通信，此方法对于更新操作来说是非线程安全的。
    private static HTable table = null;

    private static Connection connection = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("","");
        try {

            connection = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) connection.getAdmin();
            table = (HTable) connection.getTable(TableName.valueOf(TABLE_NAME));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //创建一个表(admin操作)
    public void createTable(String tableName,String[] family) throws IOException {
        //指定表名
        HTableDescriptor htd = new HTableDescriptor(tableName);
        //指定列簇名
        for(String cf : family){
            HColumnDescriptor hcd = new HColumnDescriptor(cf);
            //增加列簇
            htd.addFamily(hcd);
        }
        admin.createTable(htd);
    }

    //找到所有的表(admin操作)
    public void getAllTables() throws IOException {
        //查询所有的数据表，返回的是整个表信息组成的一个数组。包含了表的名字及其对应的列簇。
        HTableDescriptor[] listTables = admin.listTables();
        for(HTableDescriptor htd : listTables){
            //打印出每个表名
            System.out.println(htd.getTableName().toString());
        }
    }

    //查看一个表的信息
    public boolean existTable(String tableName) throws IOException {
        boolean tableExist = admin.tableExists(tableName);
        return tableExist;
    }

    //使表失效(admin操作)
    public void disableTable(String tableName) throws Exception {
        admin.disableTable(tableName);
    }
    //删除表(admin操作)
    public void dropTable(String tableName) throws Exception {

        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    //查看一个表的信息(table操作)
    public void descTable(String tableName) throws IOException {
      //获取一个表的实例,可以用来和HBase表直接通信，此方法对于更新操作来说是非线程安全的。
      table = (HTable) connection.getTable(TableName.valueOf(tableName));
      //返回一个表的所有数据
      HTableDescriptor htd = table.getTableDescriptor();
      //返回表中所有的列簇
      HColumnDescriptor[] columnFamilies = htd.getColumnFamilies();
      for (HColumnDescriptor hcd : columnFamilies) {
          //打印所有的列簇
          System.out.println(hcd.getNameAsString());
      }
    }

    //查询数据(table操作)
    public Result getResult(String tableName,String rowkey) throws IOException {
        table = (HTable)connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowkey.getBytes());
        //返回一个result结果集(由rowkey确定)
        Result result = table.get(get);
        //通过result得到一个cell的集合
        List<Cell> cells = result.listCells();
        for(Cell cell : cells){
            //分别对每个cell打印
            System.out.println(new String(cell.getRow())+"\t"+
                    new String(cell.getFamily())+"\t"+
                    new String(cell.getQualifier())+"\t"+
                    new String(cell.getValue())+"\t"+
                    cell.getTimestamp());
        }
        return result;
    }

    //插入数据(table操作)
    public void  putData(String tableName,String rowKey,String familyName,String columnName,String value,long timestamp) throws IOException {
        // 表名， rowkey, 列簇， 列名， 值， 时间戳
        table = (HTable) connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(familyName.getBytes(), columnName.getBytes(), timestamp, value.getBytes());
        table.put(put);
    }

    //修改表(修改列簇 目的：删除 cf1, 添加 cf3)(admin操作)
    public void updateTable(String tableName) throws IOException {
        table = (HTable)connection.getTable(TableName.valueOf(tableName));
        //获取表的信息
        HTableDescriptor htd = table.getTableDescriptor();
        //获取表中的所有列簇
        HColumnDescriptor[] columnDescriptors =htd.getColumnFamilies();
        //创建一个新的HTableDescriptor  因为原有的只可读不能修改
        HTableDescriptor htd2 = new HTableDescriptor(tableName);
        for(HColumnDescriptor hcd:columnDescriptors){
            //如果是cf1则不做任何操作
            if("cf1".equals(hcd.getNameAsString())){
            }else{
                //如果不是则加入到新的HTableDescriptor
                htd2.addFamily(hcd);
            }
        }
        //最后增加一个列簇
        htd2.addFamily(new HColumnDescriptor("cf4"));
        //修改表
        admin.modifyTable(tableName,htd);
    }

    //删除数据(table操作)
    public void deleteColumns(String tableName,String rowKey) throws IOException {
        //删除所有
        HBaseDemo hbase = new HBaseDemo();
        table = (HTable) connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        Result result = hbase.getResult(tableName, rowKey);
        List<Cell> listCells = result.listCells();
        for(Cell cell:listCells){
            delete.addColumn(cell.getFamily(),cell.getQualifier());
        }
        table.delete(delete);
    }

    public void deleteColumn(String tableName, String rowKey, String falilyName, String columnName) throws Exception {
        //删除指定
        table = (HTable) connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        delete.addColumn(falilyName.getBytes(), columnName.getBytes());
        table.delete(delete);
    }

    //全表扫描
    public ResultScanner getResultScanner(String tableName) throws IOException {
        table = (HTable) connection.getTable(TableName.valueOf(tableName));
        //获取一个scan实例
        Scan scan = new Scan();
        //返回一个scanner结果集，里面包含n个result;
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner){
            List<Cell> cells = result.listCells();
            for(Cell cell:cells){
                System.out.println(new String(cell.getRow())+"\t"+
                        new String(cell.getFamily())+"\t"+
                        new String(cell.getQualifier())+"\t"+
                        new String(cell.getValue())+"\t"+
                        cell.getTimestamp());
            }
        }
        return scanner;
    }

    public static void main(String[] args) throws IOException {
        HBaseDemo hbase = new HBaseDemo();
        hbase.getAllTables();
    }
}
