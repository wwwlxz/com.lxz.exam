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

import com.google.common.primitives.Bytes;
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
			saveToNewMistakes(userMistake);
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
	private void saveToNewMistakes(UserMistake userMistake) throws IOException{
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
	
	//create 'perMistaks','info'
	/*
	 * perMistakes表中存放的是再错率的数据，其中包括试题编号，推荐次数，再错次数，试题所属类型
	 * rowKey：试题编号
	 * family：info
	 * column：recommend 推荐次数;mistakeAgain 再错次数;type 试题所属类型
	 * 
	 */
	private void saveToPerMistakes(UserMistake userMistake) throws IOException{
		String tableName = "perMistakes";//存入的表名
		//String userID = userMistake.getUserID();//行键
		String familyName = "info";//列族名
		String mistakeID = userMistake.getMistakeID();//错题ID
		String typeID = userMistake.getTypeID();//试题所属的类型
		boolean isMistakeAgain = userMistake.isMistakeAgain();//判断是否再次出错，如果是则perMistakes表中该试题的再错次数加1，同时推荐次数加1
															//如果不是则perMistakes表中该试题的推荐次数加1
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://192.168.192.100:9000/hbase");
		conf.set("hbase.zookeeper.quorum", "192.168.192.100");
		HTable hTable = new HTable(conf, tableName);
		if(isMistakeAgain == true){
			hTable.incrementColumnValue(mistakeID.getBytes(), familyName.getBytes(), "recommend".getBytes(), 1);//推荐次数加1
			hTable.incrementColumnValue(mistakeID.getBytes(), familyName.getBytes(), "mistakeAgain".getBytes(), 1);//再错次数加1
			HBaseUtil.putRecord(hTable, mistakeID, familyName, "type", typeID);//插入该题所属的类型
		}else{
			hTable.incrementColumnValue(mistakeID.getBytes(), familyName.getBytes(), "recommend".getBytes(), 1);//推荐次数加1
			HBaseUtil.putRecord(hTable, mistakeID, familyName, "type", typeID);//插入该题所属的类型
		}
		hTable.close();
	}
}
