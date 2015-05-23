package com.lxz.util;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class HBaseUtil {
	public static final String TABLE_NAME = "table";
	public static final String FAMILY_NAME = "family";
	public static final String ROW_KEY = "row_key";
	
	public static void main(String[] args) throws IOException{
		Configuration conf = HBaseConfiguration.create();
//		conf.set("hbase.rootdir", "hdfs://115.29.211.139:9000/hbase");
		conf.set("hbase.rootdir", "hdfs://192.168.192.100:9000/hbase");
		//使用eclipse时必须添加这个，否则无法定位
//		conf.set("hbase.zookeeper.quorum", "115.29.211.139");
		conf.set("hbase.zookeeper.quorum", "192.168.192.100");
		
		//创建表、删除表使用HBaseAdmin
		final HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
//		createTable(hBaseAdmin, TABLE_NAME, FAMILY_NAME);
		
		//插入记录、查询一条记录、遍历所有的记录HTable
		final HTable hTable = new HTable(conf ,TABLE_NAME);
		//插入记录
//		putRecord(hTable, "hello", FAMILY_NAME, "age", "25");
		//查询一条记录
//		getRecord(hTable, ROW_KEY, FAMILY_NAME, "age");
		//遍历所有的记录
		scanTable(hTable, FAMILY_NAME);
		hTable.close();
		
//		deleteTable(hBaseAdmin, TABLE_NAME);
	}
	
	//查看表中有哪些数据
	/*
	 * @descriptor scan the whole table
	 * @param HTable,familyName,column
	 */
	public static void scanTable(final HTable hTable, String familyName) throws IOException{
		Scan scan = new Scan();
		final ResultScanner scanner = hTable.getScanner(scan);
		for(Result result : scanner){
			for(Map.Entry<byte[], byte[]> entry : result.getFamilyMap(familyName.getBytes()).entrySet()){
				String column = new String(entry.getKey());
				String value = new String(entry.getValue());
				System.out.println("---------------");
				System.out.println(column + "\t" + value);
				System.out.println("---------------");
			}
		}
	}
	
	/*
	 * @descriptor get a record from a table
	 * @param HTable,rowKey,familyName,column
	 * @return
	 */
	public static void getRecord(final HTable hTable, String rowKey, String familyName, String column) throws IOException{
		Get get = new Get(rowKey.getBytes());
		final Result result = hTable.get(get);
		final byte[] value = result.getValue(familyName.getBytes(), column.getBytes());
//		final byte[] value = result.getValue(FAMILY_NAME.getBytes(), "age".getBytes());
		System.out.println(result + "\t" + new String(value));
	}
	
	/*
	 * @descriptor put a record into a table
	 * @param HTabel,rowkey,familyName,column,value
	 * @retrun 
	 */
	public static void putRecord(final HTable hTable, String rowKey, String familyName, String column, String value) throws IOException{
		Put put = new Put(rowKey.getBytes());
		put.add(familyName.getBytes(), column.getBytes(), value.getBytes());
//		put.add(FAMILY_NAME.getBytes(), "age".getBytes(), "25".getBytes());
//		put.add(FAMILY_NAME.getBytes(), "abc".getBytes(), "15".getBytes());
//		put.add(FAMILY_NAME.getBytes(), "efg".getBytes(), "81".getBytes());
		hTable.put(put);
	}
	
	/*
	 * @descriptor delete a table
	 * @param HBaseAdmin,tablename
	 * @return 
	 */
	public static void deleteTable(final HBaseAdmin hBaseAdmin, String tableName) throws IOException{
		hBaseAdmin.disableTable(tableName);
		hBaseAdmin.deleteTable(tableName);
	}
	
	//创建一张表，参数有操作表的句柄和表名、列族名
	/*
	 * @descriptor create a table
	 * @param HBaseAdmin,tablename,familyname
	 * @return
	 */
	public static void createTable(final HBaseAdmin hBaseAdmin, String tableName, String familyName) throws IOException{
		if(!hBaseAdmin.tableExists(tableName)){
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			HColumnDescriptor family = new HColumnDescriptor(familyName);
			tableDescriptor.addFamily(family);
			hBaseAdmin.createTable(tableDescriptor);
		}
	}
}
