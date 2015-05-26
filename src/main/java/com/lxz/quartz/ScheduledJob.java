package com.lxz.quartz;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

public class ScheduledJob extends QuartzJobBean{
	private AnotherBean anotherBean;
	
	public void setAnotherBean(AnotherBean anotherBean) {
		this.anotherBean = anotherBean;
	}

	@Override
	protected void executeInternal(JobExecutionContext context)
			throws JobExecutionException {
		anotherBean.printAnotherMessage();
	}

}
