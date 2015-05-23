package com.lxz.activemq.sender;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.lxz.activemq.dao.UserMistake;

public class SenderMain {
	public static void main(String[] args){
		ApplicationContext context = new ClassPathXmlApplicationContext("activemq-sender.xml");
		
		Sender frontDesk = (Sender)context.getBean("sender");
		frontDesk.sendUserMistake(new UserMistake("124", "US", "1.5", "2015-5-21"));
	}
}
