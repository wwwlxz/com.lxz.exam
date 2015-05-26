package com.lxz.quartz;

import org.springframework.stereotype.Component;

public class AnotherBean {
	public void printAnotherMessage(){
		System.out.println("I am called by Quartz jobBean using CronTriggerFactoryBean.");
	}
}
