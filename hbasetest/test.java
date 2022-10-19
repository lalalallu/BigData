package hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

/**
 * @author lalalallu
 */
public class test {
    public static Configuration conf;
    public static Connection con;
    public static Admin admin;
    public static Table table;

    public static void startCon() throws IOException {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "masternode");
        con = ConnectionFactory.createConnection(conf);
        admin = con.getAdmin();
        table = con.getTable(TableName.valueOf("Student"));
    }

    public static void close() throws IOException {
        table.close();
        admin.close();
        con.close();
    }

    public static void list() throws IOException {
        List<TableDescriptor> l = admin.listTableDescriptors();
        for (TableDescriptor i : l) {
            System.out.println(i);
        }
    }

    public static void scan(String tableName) throws IOException {
        table = con.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.print(new String(CellUtil.cloneRow(cell)) + "-" + new String(CellUtil.cloneFamily(cell)) + "-" + new String(CellUtil.cloneQualifier(cell)) + "-" + new String(CellUtil.cloneValue(cell)) + "\t");
            }
            System.out.println();
        }
    }

    public static void createTable(String tableName, String[] columnFamilies) throws IOException {
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            System.out.println("Table exists! ");
            admin.disableTable(name);
            admin.deleteTable(name);
        }
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(name);
        for (String columnFamily : columnFamilies) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }
        admin.createTable(tableDescriptorBuilder.build());
    }

    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        table = con.getTable(TableName.valueOf(tableName));
        for (int i = 0; i != fields.length; i++) {
            Put put = new Put(Bytes.toBytes(row));
            String[] cols = fields[i].split(":");
            put.addColumn(Bytes.toBytes(cols[0]), Bytes.toBytes(cols[1]), Bytes.toBytes(values[i]));
            table.put(put);
        }
    }

    public static void scanColumn(String tableName, String column) throws IOException {
        table = con.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        String[] cols = column.split(":");
        if (cols.length == 1) {
            scan.addFamily(Bytes.toBytes(cols[0]));
        } else {
            scan.addColumn(Bytes.toBytes(cols[0]), Bytes.toBytes(cols[1]));
        }
        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
                System.out.println("TimeTamp:" + cell.getTimestamp() + " ");
                System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
                System.out.println("column Qualifier:" + new String(CellUtil.cloneQualifier(cell)) + " ");
                System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
            }
        }
    }

    public static void modifyData(String tableName, String row, String column) throws IOException {
        table = con.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(row));
        System.out.println("请输入值：");
        Scanner input = new Scanner(System.in);
        String str = input.next();
        String[] cols = column.split(":");
        if (cols.length == 1) {
            put.addColumn(Bytes.toBytes(cols[0]), null, Bytes.toBytes(str));
        } else {
            put.addColumn(Bytes.toBytes(cols[0]), Bytes.toBytes(cols[1]), Bytes.toBytes(str));
        }
        table.put(put);
    }

    public static void deleteRow(String tableName, String row) throws IOException {
        table = con.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(row));
        table.delete(delete);
    }

    public static void deleteAll(String tableName) throws IOException {
        table = con.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                Delete delete = new Delete(Bytes.toBytes(new String(CellUtil.cloneRow(cell))));
                table.delete(delete);
            }
        }
    }

    public static void countRows(String tableName) throws IOException {
        table = con.getTable(TableName.valueOf(tableName));
        int count = 0;
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            count += result.size();
        }
        System.out.println(tableName + "表的行数为" + count);
    }

    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();
        startCon();
        list();
        //建表
        String[] columnFamilies = {"test01", "test02"};
        createTable("test", columnFamilies);
        //添加数据
        String[] field = {"test01:test001", "test01:test002", "test01:test003", "test02:test001", "test02:test002"};
        String[] value = {"1", "2", "3", "4", "5"};
        addRecord("test", "t001", field, value);
        scan("test");
        //扫描列族、列
        scanColumn("test", "test01:test003");
        //更改数据
        modifyData("test", "t001", "test01:test001");
        scan("test");
        //删除行
        deleteRow("test", "t001");
        scan("test");
        //写入测试行
        String[] fields = {"test01:001"};
        for (int i = 0; i < 50; i++) {
            String[] value2 = {i + ""};
            addRecord("test", i + "", fields, value2);
        }
        scan("test");
        //计数行
        countRows("test");
        //删除全部数据
        deleteAll("test");
        scan("test");
        close();
    }
}

