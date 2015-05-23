package com.lxz.activemq.receiver;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ReceiverMain {
	public static void main(String[] args){
		new ClassPathXmlApplicationContext("activemq-receiver.xml"); 
		
//		ApplicationContext context = new ClassPathXmlApplicationContext("spring03-beans-back.xml");
//		BackOffice backOffice = (BackOffice)context.getBean("backOffice");
//		Mail mail = backOffice.receiveMail();
//		System.out.println("Mail #" + mail.getMailID() + "received.");
	}
}
