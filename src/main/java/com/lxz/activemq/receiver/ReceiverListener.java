package com.lxz.activemq.receiver;

import java.io.IOException;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.springframework.jms.support.JmsUtils;

import com.lxz.activemq.dao.UserMistake;
import com.lxz.util.HBaseUtil;

public class ReceiverListener implements MessageListener{

	public void onMessage(Message message) {
		MapMessage mapMessage = (MapMessage) message;
		try{
			UserMistake userMistake = new UserMistake();
			userMistake.setUserID(mapMessage.getString("userID"));
			userMistake.setMistakeID(mapMessage.getString("mistakeID"));
			userMistake.setTypeID(mapMessage.getString("typeID"));
			userMistake.setTime(mapMessage.getString("time"));
//			displayMail(mail);
			saveToHBase(userMistake);
		}catch(JMSException e){
			throw JmsUtils.convertJmsAccessException(e);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void displayMail(UserMistake userMistake){
		System.out.println("Mail #" + userMistake.getUserID() + "received.");
		System.out.println("Mail #" + userMistake.getMistakeID() + "received.");
		System.out.println("Mail #" + userMistake.getTypeID() + "received.");
		System.out.println("Mail #" + userMistake.getTime() + "received.");
	}
	
	//create 'newMistakes','mistake'
	/*
	 * @descriptor reading the message from the ActiveMQ, and then saving it into the hbase
	 * 		create 'newMistakes','mistake'
	 * @param userMistake
	 * @return 
	 */
	private void saveToHBase(UserMistake userMistake) throws IOException{
		String tableName = "newMistakes";
		String rowKey = userMistake.getUserID();
		String familyName = "mistake";
		String column = userMistake.getMistakeID();
		String value = userMistake.getTypeID() + "@" + userMistake.getTime();
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://192.168.192.100:9000/hbase");
		conf.set("hbase.zookeeper.quorum", "192.168.192.100");
		HTable hTable = new HTable(conf, tableName);
		//插入一条数据到hbase中
		HBaseUtil.putRecord(hTable, rowKey, familyName, column, value);
		hTable.close();
	}
}
