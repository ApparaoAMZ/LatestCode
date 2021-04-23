package com.amazon.gdpr.scheduler;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.service.InitService;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This Sheduler is used to initiate the GDPR Process  
 ****************************************************************************************/
@Component
public class GdprScheduler {
	
	public static String CURRENT_CLASS = "GdprScheduler";		
	
	@Autowired
	InitService initService;
	
	/**
	 * This method navigates the GDPR_Depersonalization__c, GDPR_Employee_Depersonalization__c table
	 * and loads the SF_COPY schema with the data for depersonalization
	 * @param 
	 * @return
	 */
	@Scheduled(cron = "${gdpr.scheduler.cron.expression}")
	public void gdprProcessSchedule() throws GdprException {
		String CURRENT_METHOD = "gdprProcessSchedule";
		String currentDate = DateTimeFormatter.ofPattern(GlobalConstants.DATE_FORMAT).format(LocalDateTime.now());	
			
		String gdprProcessScheduleStatus =  initService.initService("Run "+currentDate, null);					
	}
	
}