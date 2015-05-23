package com.lxz.activemq.sender;

import com.lxz.activemq.dao.UserMistake;

public interface Sender {
	public void sendUserMistake(UserMistake userMistake);
}
