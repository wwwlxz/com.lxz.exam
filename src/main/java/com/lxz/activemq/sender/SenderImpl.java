package com.lxz.activemq.sender;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Session;

import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.core.support.JmsGatewaySupport;

import com.lxz.activemq.dao.UserMistake;

public class SenderImpl extends JmsGatewaySupport implements Sender{
	private Destination destination;
	
	public void setDestination(Destination destination){
		this.destination = destination;
	}
	
	public void sendUserMistake(final UserMistake userMistake) {
//		getJmsTemplate().convertAndSend(mail);
		
//		Map<String, Object> map = new HashMap<String, Object>();
//		map.put("mailID", mail.getMailID());
//		map.put("country", mail.getCountry());
//		map.put("weight", mail.getWeight());
//		getJmsTemplate().convertAndSend(map);
		
		getJmsTemplate().send(new MessageCreator(){
			public Message createMessage(Session session) throws JMSException{
				MapMessage message = session.createMapMessage();
				message.setString("userID", userMistake.getUserID());
				message.setString("mistakeID", userMistake.getMistakeID());
				message.setString("typeID", userMistake.getTypeID());
				message.setString("time", userMistake.getTime());
				return message;
			}
		});
	}

}
