package com.amazon.gdpr.processor;

import java.util.Date;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.dao.GdprInputDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.service.BackupService;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This Service will reorganize the GDPR_Depersonalization__c table  
 * This will be invoked by the GDPRController
 ****************************************************************************************/
@Component
public class ReOrganizeInputProcessor {

	public static String CURRENT_CLASS	= GlobalConstants.CLS_REORGANIZE_INPUT_PROCESSOR;
	
	@Autowired
	JobLauncher jobLauncher;
	
	@Autowired
    Job processreorganizeInputJob;
	
	@Autowired
    Job processReorganizeInputEmpJob;
	
	@Autowired
	RunMgmtProcessor runMgmtProcessor;

	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;

	@Autowired
	TagDataProcessor tagDataProcessor;
	
	@Autowired
	GdprInputDaoImpl gdprInputDaoImpl;
	
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	@Autowired
	BackupService backupService;
			
	public String reOrganizeData(long runId, List<String> selectedCandCountries,List<String> selectedEmpCountries) throws GdprException {
		String CURRENT_METHOD = "reOrganizeData";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
			
		JobThread jobThread = new JobThread(runId, selectedCandCountries,selectedEmpCountries);
		jobThread.start();
		return GlobalConstants.MSG_REORGANIZEINPUT_JOB;
	}
	
	class JobThread extends Thread {
		long runId;
		List<String> selectedCandCountries ;
		List<String> selectedEmpCountries ;
		
		JobThread(long runId, List<String> selectedCandCountries,List<String> selectedEmpCountries){
			this.runId = runId;
			this.selectedCandCountries = selectedCandCountries;
			this.selectedEmpCountries = selectedEmpCountries;
		}
		
		@Override
		public void run() {
			String CURRENT_METHOD = "run";
			String reOrganizeDataStatus = "";
			Boolean exceptionOccured = false;
			Date moduleStartDateTime = null;
			String errorDetails = "";
			String moduleStatus = "";
			String prevJobModuleStatus = "";
				
			if((selectedCandCountries != null && selectedCandCountries.size() > 0) ||(selectedEmpCountries != null && selectedEmpCountries.size() > 0 )) {
					try {
		    		moduleStartDateTime = new Date();	    				    		
					for(String currentCountry : selectedCandCountries) { 	    				    		
						JobParametersBuilder jobParameterBuilder= new JobParametersBuilder();
						jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_RUN_ID, runId);
						jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_JOB_ID, new Date().getTime());
						jobParameterBuilder.addDate(GlobalConstants.JOB_INPUT_START_DATE, new Date());
						jobParameterBuilder.addString(GlobalConstants.JOB_INPUT_COUNTRY_CODE, currentCountry);
						jobParameterBuilder.addString(GlobalConstants.JOB_INPUT_RECORDTYPE, GlobalConstants.JOB_INPUT_CAN_RECORD);
						
						System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: JobParameters set ");
						JobParameters jobParameters = jobParameterBuilder.toJobParameters();
		
						jobLauncher.run(processreorganizeInputJob, jobParameters);
					}
					reOrganizeDataStatus = GlobalConstants.MSG_CAN_REORGANIZEINPUT_JOB;
		    		
		    						
				} catch (JobExecutionAlreadyRunningException | JobRestartException
						| JobInstanceAlreadyCompleteException | JobParametersInvalidException exception) {
					exceptionOccured = true;
					reOrganizeDataStatus = GlobalConstants.ERR_CAN_REORGANIZE_JOB_RUN;
					System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
					exception.printStackTrace();
					errorDetails = exception.getStackTrace().toString();
				}
				if (!exceptionOccured) {
					try {						
						for(String currentCountry : selectedEmpCountries) { 	    				    	
							JobParametersBuilder jobParameterBuilder= new JobParametersBuilder();
							jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_RUN_ID, runId);
							jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_JOB_ID, new Date().getTime());
							jobParameterBuilder.addDate(GlobalConstants.JOB_INPUT_START_DATE, new Date());
							jobParameterBuilder.addString(GlobalConstants.JOB_INPUT_COUNTRY_CODE, currentCountry);
							jobParameterBuilder.addString(GlobalConstants.JOB_INPUT_RECORDTYPE, GlobalConstants.JOB_INPUT_EMP_RECORD);
							
							System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: JobParameters set ");
							JobParameters jobParameters = jobParameterBuilder.toJobParameters();
			
							jobLauncher.run(processReorganizeInputEmpJob, jobParameters);
						}
						reOrganizeDataStatus = reOrganizeDataStatus + GlobalConstants.MSG_EMP_REORGANIZEINPUT_JOB;	    		
		    						
					} catch (JobExecutionAlreadyRunningException | JobRestartException
							| JobInstanceAlreadyCompleteException | JobParametersInvalidException exception) {
						exceptionOccured = true;
						reOrganizeDataStatus = reOrganizeDataStatus+ GlobalConstants.ERR_EMP_REORGANIZE_JOB_RUN;
						System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
						exception.printStackTrace();
						errorDetails = exception.getStackTrace().toString();
					}
				}
			} else {
    			reOrganizeDataStatus = "No countries selected to reorganize. ";
    		}			
	    	try {
				prevJobModuleStatus = moduleMgmtProcessor.prevJobModuleStatus(runId);				
				moduleStatus = (exceptionOccured || prevJobModuleStatus.equalsIgnoreCase(GlobalConstants.STATUS_FAILURE)) ? 
						GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, 
						GlobalConstants.SUB_MODULE_REORGANIZE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime, 
						new Date(), reOrganizeDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);	
				if(exceptionOccured || prevJobModuleStatus.equalsIgnoreCase(GlobalConstants.STATUS_FAILURE)){
					runMgmtDaoImpl.updateRunStatus(runId, GlobalConstants.STATUS_FAILURE, reOrganizeDataStatus);
				}else{
					runMgmtDaoImpl.updateRunComments(runId, reOrganizeDataStatus);
				}
			} catch(GdprException exception) {
				exceptionOccured = true;
				reOrganizeDataStatus = reOrganizeDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
			}
	    	try {
	    		if((! exceptionOccured) && GlobalConstants.STATUS_SUCCESS.equalsIgnoreCase(prevJobModuleStatus)){
	    			
	    			backupService.backupServiceInitiate(runId);
	    			//tagDataProcessor.taggingInitialize(runId);
					
				}
			} catch(Exception exception) {
				exceptionOccured = true;
				reOrganizeDataStatus = reOrganizeDataStatus + exception.getMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
			}
		}
	}	
}