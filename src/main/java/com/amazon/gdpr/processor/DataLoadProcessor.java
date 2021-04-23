package com.amazon.gdpr.processor;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.dao.GdprInputFetchDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.model.gdpr.input.GdprHerokuTagging;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This processor Fetches and loads the data from the Data_Load Table
 ****************************************************************************************/
@Component
public class DataLoadProcessor {
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_DATALOAD_PROCESSOR;
			
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
	
	@Autowired
	GdprInputFetchDaoImpl gdprInputFetchDaoImpl;
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	/**
	 * After the load of each table the Data_Load Table is loaded
	 * @param dataLoad The details of the DataLoad are passed on as input
	 */
	public void updateDataLoad(long runId) throws GdprException {
		String CURRENT_METHOD = "updateDataLoad";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Data Load update in progress.");
		
		try {
			gdprOutputDaoImpl.updateDataLoad(runId);
		} catch(Exception exception) {
			String dataLoadStatus = GlobalConstants.ERR_DATA_LOAD_UPDATE;
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Exception occured");
			exception.printStackTrace();
			String errorDetails = exception.getMessage();
			throw new GdprException(dataLoadStatus, errorDetails); 
		}		
	}
	
	/**
	 * This method verifies Odaseva of the new runs and fetches the data from 
	 * Salesforce schema - GDPR_Depersonalization__c, GDPR_Employee_Depersonalization__c tables and loads into 
	 * SF_COPY schema - GDPR_Depersonalization__c, GDPR_Employee_Depersonalization__c tables respectively
	 * @param RunId is returned back to controller and this is passed on to all methods 
	 */
	public String odasevaRunCheck(long runId) throws GdprException { 
		String CURRENT_METHOD = "dataCheck"; 
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		
		Boolean exceptionOccured = false;
		Date moduleStartDateTime = null;
		String errorDetails = "";
		String odasevaRunStatus = "";
		
		try {
			moduleStartDateTime = new Date();
			List<GdprHerokuTagging> lstGdprHerokuTagging = gdprInputFetchDaoImpl.fetchGdprHerokuTagging();
			if(lstGdprHerokuTagging != null && lstGdprHerokuTagging.size() > 0) {
				odasevaRunStatus = GlobalConstants.MSG_ODASEVA_RUN_DATA_EXIST;							
			} else {
				odasevaRunStatus = GlobalConstants.MSG_ODASEVA_RUN_NO_DATA_EXIST;
			}
		} catch (Exception exception) {
			odasevaRunStatus = GlobalConstants.ERR_GDPR_ODASEVA_RUN_STATUS_CHECK;
			exceptionOccured = true;
			errorDetails = exception.getMessage();
		}
		try { 
			String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
			RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, 
					GlobalConstants.SUB_MODULE_ODASEVA_CHECK_INITIALIZE, moduleStatus, moduleStartDateTime, 
					new Date(), odasevaRunStatus, errorDetails);
			moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			
		} catch(GdprException exception) {
			exceptionOccured = true;
			exception.printStackTrace();
			errorDetails = errorDetails + exception.getMessage();
			odasevaRunStatus = odasevaRunStatus + GlobalConstants.ERR_MODULE_MGMT_INSERT;
			throw new GdprException(odasevaRunStatus, errorDetails);
		}
		
		try {
			if(GlobalConstants.MSG_ODASEVA_RUN_NO_DATA_EXIST.equalsIgnoreCase(odasevaRunStatus)) {
				runMgmtDaoImpl.updateRunStatus(runId, GlobalConstants.STATUS_NODATA, odasevaRunStatus);				
			}
			if(exceptionOccured)
				throw new GdprException(odasevaRunStatus, errorDetails);
		} catch(GdprException exception) {
			exceptionOccured = true;
			exception.printStackTrace();
			errorDetails = errorDetails + exception.getMessage();
			odasevaRunStatus = odasevaRunStatus + GlobalConstants.ERR_RUN_MGMT_UPDATE;
			throw new GdprException(odasevaRunStatus, errorDetails);
		}
		return odasevaRunStatus;
	}
	
	public List<String> fetchListCountries(long runId) throws GdprException {
		Boolean exceptionOccured = false;
		String fetchCountriesStatus = "";
		List<String> lstCountries;
		
		try {
			lstCountries = gdprInputFetchDaoImpl.fetchCountries(runId);							
		} catch(Exception exception) {
			exceptionOccured = true;
			exception.printStackTrace();
			fetchCountriesStatus = fetchCountriesStatus + GlobalConstants.ERR_GDPR_COUNTRIES_FETCH;
			throw new GdprException(fetchCountriesStatus, exception.getMessage());
		}
		return lstCountries;
	}	
}