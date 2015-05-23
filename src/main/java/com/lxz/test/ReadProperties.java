package com.lxz.test;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class ReadProperties {
	public static void main(String[] args) throws ConfigurationException{
		Configuration config = new PropertiesConfiguration("config.properties");
		String name = config.getString("hbase.rootdir");
		System.out.println(name);
	}
}
