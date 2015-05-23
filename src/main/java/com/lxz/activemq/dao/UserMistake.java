package com.lxz.activemq.dao;

public class UserMistake {
	private String userID;//用户编号
	private String mistakeID;//出错的试题号
	private String typeID;//出错的试题类型
	private String time;//出错的时间

	public UserMistake(){
	}
	
	public UserMistake(String userID, String mistakeID, String typeID, String time){
		this.userID = userID;
		this.mistakeID = mistakeID;
		this.typeID = typeID;
		this.time = time;
	}

	public String getUserID() {
		return userID;
	}

	public void setUserID(String userID) {
		this.userID = userID;
	}

	public String getMistakeID() {
		return mistakeID;
	}

	public void setMistakeID(String mistakeID) {
		this.mistakeID = mistakeID;
	}

	public String getTypeID() {
		return typeID;
	}

	public void setTypeID(String typeID) {
		this.typeID = typeID;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

}
