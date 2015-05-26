package com.lxz.quartz;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * 定时任务
 */
public class MyBean { 
	public void printMessage(){
		System.out.println("I am called by MethodInvokingJobDetailFactoryBean using SimpleTriggerFactoryBean.");
	}
	
	/*
	 * 将newMistakes表中的数据读取并写到perMistakes表中
	 */
	public void readNewMistakesToPerMistakes() throws IOException{
		String newMistakesTableName = "newMistakes";
		String newMistakesFamilyName = "mistake";
		
		String perMistakesTableName = "perMistakes";
		String perMistakesFamilyName = "info";
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://192.168.192.100:9000/hbase");
		conf.set("hbase.zookeeper.quorum", "192.168.192.100");
		HTable perMistakesHTable = new HTable(conf, perMistakesTableName);
		HTable newMistakesHTable = new HTable(conf, newMistakesTableName);
		
		//从newMistakes表中读取出数据，并将数据存放到perMistakes表中
		Scan scan = new Scan();
		final ResultScanner scanner = newMistakesHTable.getScanner(scan);
		for(Result result : scanner){
			for(Map.Entry<byte[], byte[]> entry : result.getFamilyMap(newMistakesFamilyName.getBytes()).entrySet()){
				String column = new String(entry.getKey());//存放的是试题编号
				String value = new String(entry.getValue());//存放的是试题类型@时间
				System.out.println("---------------");
				System.out.println(column + "\t" + value);
				System.out.println("---------------");
				perMistakesHTable.incrementColumnValue(Bytes.toBytes("1"), Bytes.toBytes("info"), Bytes.toBytes("abc"), 1);
				//perMistakesHTable.incrementColumnValue("1".getBytes(), "info".getBytes(), "abc".getBytes(), 1);
				//hTable.incrementColumnValue("perMistakes".getBytes(), "info".getBytes(), "abc".getBytes(), 1);
				//hTable.incrementColumnValue(Bytes.toBytes("perMistakes"), Bytes.toBytes("info"), Bytes.toBytes("abc"), 1);
				//newMistakesHTable.incrementColumnValue(Bytes.toBytes("newMistakes"), Bytes.toBytes("mistake"), Bytes.toBytes("abc"), 1);
//				Put put = new Put(column.getBytes());
//				put.add(perMistakesFamilyName.getBytes(), , value);
			}
		}
		//
		newMistakesHTable.close();
		perMistakesHTable.close();
	}
	
	/*
	 * 将perMistakes表中的数据整理写入到typesPerMistakes表中
	 */
	public void arrangeTypesPerMsitakes(){
		
	}
	
	public static void main(String[] args) throws IOException{
		new MyBean().readNewMistakesToPerMistakes();
	}
}
