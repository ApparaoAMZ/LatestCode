package com.amazon.gdpr.service;

import java.time.LocalTime;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazon.gdpr.dao.BackupServiceDaoImpl;
import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.processor.AnonymizationFileProcessor;
import com.amazon.gdpr.processor.BackupTableProcessor;
import com.amazon.gdpr.processor.DataLoadProcessor;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.processor.ReOrganizeInputProcessor;
import com.amazon.gdpr.processor.RunMgmtProcessor;
import com.amazon.gdpr.processor.SummaryDataProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.view.GdprInput;

/****************************************************************************************
 * This Service performs the Initialization activity on the Heroku GDPR  
 * This will be invoked by the GDPRController
 ****************************************************************************************/
@Service
public class InitService {
	
	public static String CURRENT_CLASS	= GlobalConstants.MODULE_INITIALIZATION;
	public static String STATUS_SUCCESS = GlobalConstants.STATUS_SUCCESS;
	
	public Map<String, RunSummaryMgmt> mapRunSummaryMgmt=null;
	private long runId = 0;
	
	@Autowired
	RunMgmtProcessor runMgmtProcessor;
	
	@Autowired
	DataLoadProcessor dataLoadProcessor;
	
	@Autowired
	AnonymizationFileProcessor anonymizationFileProcessor;

	@Autowired
	BackupTableProcessor bkpupTableProcessor;
	
	@Autowired
	ReOrganizeInputProcessor reOrganizeInputProcessor;
	
	@Autowired
	SummaryDataProcessor summaryDataProcessor;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	@Autowired
	BackupService backupService;
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	@Autowired
	BackupServiceDaoImpl backupServiceDaoImpl;
	/**
	 * This method initiates the InitService activities
	 * @param runName
	 * @return
	 */
	public String initService(String runName, List<String> selectedCountries) {
		String CURRENT_METHOD = "initService";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		
		String initServiceReturnStatus = "";
		Boolean exceptionOccured = false;
		String prevModuleStatus = ""; 
		try {
			//Initiates the run. Establishes the run in the DB
			runId =  runMgmtProcessor.initializeRun(runName);
			initServiceReturnStatus = runMgmtProcessor.initializeRunStatus;
			Boolean oldRun = runMgmtProcessor.oldRun; 
			if(! oldRun) {
				String odasevaRunStatus = dataLoadProcessor.odasevaRunCheck(runId);
				initServiceReturnStatus = initServiceReturnStatus + GlobalConstants.SEMICOLON_STRING + odasevaRunStatus;
				if(GlobalConstants.MSG_ODASEVA_RUN_DATA_EXIST.equalsIgnoreCase(odasevaRunStatus)) {	
					String depersBackupStatus=backupService.backupDepersonalizationTables(runId);
					initServiceReturnStatus = initServiceReturnStatus + GlobalConstants.SEMICOLON_STRING + depersBackupStatus;
					if(depersBackupStatus.contains(GlobalConstants.MSG_BACKUPSERVICE_DEPERSONALIZETABLE_DATA)) {
					List<String> lstCountry =  dataLoadProcessor.fetchListCountries(runId);
					List<String> lstEmpCountry =  dataLoadProcessor.fetchListEmpCountries(runId);
					
					System.out.println("lstCountry:::"+lstCountry);
					System.out.println("lstEmpCountry:::"+lstEmpCountry);
					String[] initServiceStatus = initialize(runId, lstCountry,lstEmpCountry);
					initServiceReturnStatus = initServiceReturnStatus + GlobalConstants.SEMICOLON_STRING + initServiceStatus[1];
					}
					else
					{
						try {
							prevModuleStatus = moduleMgmtProcessor.prevJobModuleStatus(runId);
							if (GlobalConstants.STATUS_SUCCESS.equalsIgnoreCase(prevModuleStatus)) {
								dataLoadProcessor.updateDataLoad(runId);				
								initServiceReturnStatus = initServiceReturnStatus + GlobalConstants.SEMICOLON_STRING + GlobalConstants.MSG_DATALOAD_DTLS;
				    		}
							if(exceptionOccured || GlobalConstants.STATUS_FAILURE.equalsIgnoreCase(prevModuleStatus)){					
								runMgmtDaoImpl.updateRunStatus(runId, GlobalConstants.STATUS_FAILURE, initServiceReturnStatus);
							}else {					
								runMgmtDaoImpl.updateRunStatus(runId, GlobalConstants.STATUS_SUCCESS, initServiceReturnStatus);
							}
							if(GlobalConstants.STATUS_SUCCESS.equalsIgnoreCase(prevModuleStatus)){					
							String herokuUpdate = "UPDATE SALESFORCE.GDPR_HEROKU_TAGGING__C SET HEROKU_STATUS__C = \'"
			    					+GlobalConstants.STATUS_CLEARED+"\' WHERE HEROKU_STATUS__C = '' OR HEROKU_STATUS__C IS NULL";
							int herokuStatus = backupServiceDaoImpl.gdprHerokuStatusUpdate(herokuUpdate);
							System.out.println("herokuUpdate::"+herokuUpdate);
							}
							System.out.println("initServiceReturnStatus::prevModuleStatus"+initServiceReturnStatus+"::"+prevModuleStatus);
						} catch(Exception exception) {
							exceptionOccured = true;
							exception.printStackTrace();
							initServiceReturnStatus = initServiceReturnStatus + GlobalConstants.ERR_RUN_MGMT_UPDATE;
							System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+initServiceReturnStatus);
						}
					}
				}				
			} 
		} catch(GdprException exception) {
			exceptionOccured = true;
			initServiceReturnStatus = initServiceReturnStatus + exception.getExceptionMessage();
		}
		try {
			if(exceptionOccured) { 
				runMgmtDaoImpl.updateRunStatus(runId, GlobalConstants.STATUS_FAILURE, initServiceReturnStatus);
			} else { 
				runMgmtDaoImpl.updateRunComments(runId, initServiceReturnStatus);
			}
		} catch(Exception exception) {
			exceptionOccured = true;
			initServiceReturnStatus = initServiceReturnStatus + GlobalConstants.ERR_RUN_MGMT_UPDATE;
		}
		if (exceptionOccured)
			initServiceReturnStatus = initServiceReturnStatus + GlobalConstants.ERR_DISPLAY;
		return initServiceReturnStatus;
	}
	
	/**
	 * The initialization activities are performed. Verification of all the basic steps required for the project
	 * @param runId
	 * @return
	 */
	public String[] initialize(long runId, List<String> selectedCandCountries,List<String> selectedEmpCountries) throws GdprException {
		
		String CURRENT_METHOD = "initialize";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		String[] initializationStatus = new String[2];
		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before Anonymization Processor : "+LocalTime.now());
		int insertRunAnonymizationCounts = anonymizationFileProcessor.loadRunAnonymization(runId, selectedCandCountries,selectedEmpCountries);		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After Anonymization Processor : "+LocalTime.now());
		if(insertRunAnonymizationCounts == 0) {
			initializationStatus[0] = GlobalConstants.STATUS_FAILURE;
			initializationStatus[1] = GlobalConstants.RUN_ANONYMIZATION_ZERO;
		}
		else {
			initializationStatus[0] = GlobalConstants.STATUS_SUCCESS;
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before Backup Processor : "+LocalTime.now());
			String processBackupTableStatus = bkpupTableProcessor.processBkpupTable(runId);
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After Backup Processor : "+LocalTime.now());
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before Summary Processor : "+LocalTime.now());			
			String summaryStatus = summaryDataProcessor.processSummaryData(runId);
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After Summary Processor : "+LocalTime.now());
			//String depersBackupStatus=backupService.backupDepersonalizationTables(runId);
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before GDPR Data Processor : "+LocalTime.now());
			String reOrganizeDataStatus = reOrganizeInputProcessor.reOrganizeData(runId, selectedCandCountries,selectedEmpCountries);
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After GDPR Data Processor : "+LocalTime.now());			
						
			initializationStatus[1] = GlobalConstants.RUN_ANONYMIZATION_INSERT + insertRunAnonymizationCounts +
					GlobalConstants.SEMICOLON_STRING + processBackupTableStatus + GlobalConstants.SEMICOLON_STRING + summaryStatus + 
					GlobalConstants.SEMICOLON_STRING + reOrganizeDataStatus;
		}
		return initializationStatus;
	}
	
	/**
	 * @return the runId
	 */
	public long getRunId() {
		return runId;
	}

	/**
	 * @param runId the runId to set
	 */
	public void setRunId(long runId) {
		this.runId = runId;
	}
		
	public GdprInput loadGdprForm() {
		GdprInput gdprInput = new GdprInput();
		
		try{
			gdprInput = runMgmtProcessor.loadCountryDetail();
		} catch(GdprException exception) {
			gdprInput.setErrorStatus(GlobalConstants.STATUS_FAILURE);
			gdprInput.setRunStatus(GlobalConstants.ERR_LOAD_FORM);
		}
		return gdprInput;
	}
}