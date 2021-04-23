package com.amazon.gdpr.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazon.gdpr.dao.BackupServiceDaoImpl;
import com.amazon.gdpr.dao.BackupTableProcessorDaoImpl;
import com.amazon.gdpr.dao.GdprInputDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.model.gdpr.output.BackupTableDetails;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.processor.RunMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This Service performs the Depersonalization activity on the Heroku Backup
 * Data This will be invoked by the GDPRController
 ****************************************************************************************/
@Service
public class BackupService {
	public static String CURRENT_CLASS	= GlobalConstants.CLS_BACKUPSERVICE;
	public static String MODULE_DATABACKUP = GlobalConstants.MODULE_DATABACKUP;
	public static String STATUS_SUCCESS = GlobalConstants.STATUS_SUCCESS;

	@Autowired
	JobLauncher jobLauncher;

	@Autowired
	Job processGdprBackupServiceJob;

	@Autowired
	RunMgmtProcessor runMgmtProcessor;

	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;

	@Autowired
	GdprInputDaoImpl gdprInputDaoImpl;

	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	@Autowired
	public BackupServiceDaoImpl backupServiceDaoImpl;
	
	@Autowired
	private BackupTableProcessorDaoImpl backupTableProcessorDaoImpl;
	
	public String backupServiceInitiate(long runId) {
		String CURRENT_METHOD = "backupServiceInitiate";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
			
		BackupJobThread bkpJobThread = new BackupJobThread(runId);
		bkpJobThread.start();
		return GlobalConstants.MSG_BACKUPSERVICE_JOB;
	}

	
	class BackupJobThread extends Thread {
		long runId;

		BackupJobThread(long runId) {
			this.runId = runId;
		}

		@Override
		public void run() {
			String CURRENT_METHOD = "run";
			String backupServiceStatus = "";
			Boolean exceptionOccured = false;
			Date moduleStartDateTime = null;			
			String moduleStatus="";
			String errorDetails = "";
			String prevJobModuleStatus = "";
			
			try {
				moduleStartDateTime = new Date();

				JobParametersBuilder jobParameterBuilder = new JobParametersBuilder();
				jobParameterBuilder.addLong(GlobalConstants.JOB_BACKUP_SERVICE_INPUT_RUNID, runId);
				jobParameterBuilder.addLong(GlobalConstants.JOB_BACKUP_SERVICE_INPUT_JOBID, new Date().getTime());
				jobParameterBuilder.addDate(GlobalConstants.JOB_INPUT_START_DATE, new Date());

				System.out.println(MODULE_DATABACKUP + " ::: " + CURRENT_METHOD + " :: JobParameters set ");
				JobParameters jobParameters = jobParameterBuilder.toJobParameters();

				jobLauncher.run(processGdprBackupServiceJob, jobParameters);
				backupServiceStatus = GlobalConstants.MSG_BACKUPSERVICE_JOB;
			} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
					| JobParametersInvalidException exception) {
				exceptionOccured = true;
				backupServiceStatus = GlobalConstants.ERR_BACKUPSERVICE_JOB_RUN;
				System.out.println(MODULE_DATABACKUP + " ::: " + CURRENT_METHOD + " :: " + backupServiceStatus);
				exception.printStackTrace();
				errorDetails = exception.getStackTrace().toString();
			}
			try {
				prevJobModuleStatus = moduleMgmtProcessor.prevJobModuleStatus(runId);				
				moduleStatus = (exceptionOccured || prevJobModuleStatus.equalsIgnoreCase(GlobalConstants.STATUS_FAILURE)) ? 
						GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_BACKUPSERVICE,
						GlobalConstants.SUB_MODULE_BACKUPSERVICE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime,
						new Date(), backupServiceStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);				
			} catch (GdprException exception) {
				exceptionOccured = true;
				backupServiceStatus = backupServiceStatus + exception.getExceptionMessage();
				System.out.println(MODULE_DATABACKUP + " ::: " + CURRENT_METHOD + " :: " + backupServiceStatus);
			}
			try {
				if(exceptionOccured || GlobalConstants.STATUS_FAILURE.equalsIgnoreCase(moduleMgmtProcessor.prevJobModuleStatus(runId))){
					runMgmtDaoImpl.updateRunStatus(runId, GlobalConstants.STATUS_FAILURE, backupServiceStatus);
				}else{
					runMgmtDaoImpl.updateRunComments(runId, backupServiceStatus);
				}
			} catch(Exception exception) {
				exceptionOccured = true;
				backupServiceStatus = backupServiceStatus + exception.getMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+backupServiceStatus);
			}
		}
	}
	public String backupDepersonalizationTables(long runId) {
		String CURRENT_METHOD = "backupDepersonalizationTables";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		Boolean exceptionOccured = false;
		List<BackupTableDetails> lstBackupTableDetails = null;
		Map<String, List<String>> mapbackuptable = null;
		String selectColumns ="";
		List<String> lstCols = new ArrayList<String>();
		long insertcount = 0;
		lstBackupTableDetails = backupTableProcessorDaoImpl.fetchSFCOPYTableDetails();
		mapbackuptable = lstBackupTableDetails.stream().collect(Collectors.toMap(BackupTableDetails::getBackupTableName, bkp -> {
			List list = new ArrayList<String>();
			list.add(bkp.getBackupTablecolumn());
			return list;
		}, (s, a) -> {
			s.add(a.get(0));
			return s;
		}));
		
		System.out.println(" :::mapbackuptable:: "+mapbackuptable+":: Inside method");
		
		String strLastFetchDate = gdprOutputDaoImpl.fetchLastDataLoadedDate();
		try {
		
		if(mapbackuptable.containsKey(GlobalConstants.TBL_GDPR_DEPERSONALIZATION__C)) {
			lstCols=mapbackuptable.get(GlobalConstants.TBL_GDPR_DEPERSONALIZATION__C.toLowerCase());	
			lstCols.remove("run_id");
			selectColumns = lstCols.stream().map(String::valueOf).collect(Collectors.joining(","));
			System.out.println(" :::selectColumns:: "+selectColumns+":: Inside method");
		    String backupDataInsertQuery = "INSERT INTO SF_COPY.GDPR_DEPERSONALIZATION__C (RUN_ID," + selectColumns + ")  select "+runId+" RUN_ID,"+selectColumns+" FROM SF_ARCHIVE.GDPR_DEPERSONALIZATION__C WHERE TO_CHAR(CREATEDDATE, 'YYYY-MM-DD') >='"+strLastFetchDate+"' OR TO_CHAR(CREATEDDATE, 'YYYY-MM-DD') >='"+strLastFetchDate+"'";
		    System.out.println(" :::backupDataInsertQuery:: "+backupDataInsertQuery+":: Inside method");
		    insertcount = backupServiceDaoImpl.insertBackupTable(backupDataInsertQuery);
		} 
		
		if(mapbackuptable.containsKey(GlobalConstants.TBL_GDPR_EMPLOYEE_DEPERSONALIZATION__C)) {
			lstCols=mapbackuptable.get(GlobalConstants.TBL_GDPR_EMPLOYEE_DEPERSONALIZATION__C.toLowerCase());	
			lstCols.remove("run_id");
			selectColumns = lstCols.stream().map(String::valueOf).collect(Collectors.joining(","));
			System.out.println(" :::selectColumns:: "+selectColumns+":: Inside method");
		    String backupDataInsertQuery = "INSERT INTO SF_COPY.GDPR_EMPLOYEE_DEPERSONALIZATION__C (RUN_ID," + selectColumns + ")  select "+runId+" RUN_ID,"+selectColumns+" FROM SF_ARCHIVE.GDPR_EMPLOYEE_DEPERSONALIZATION__C WHERE TO_CHAR(CREATEDDATE, 'YYYY-MM-DD')>='"+strLastFetchDate+"' OR TO_CHAR(CREATEDDATE, 'YYYY-MM-DD') >='"+strLastFetchDate+"'";
		    System.out.println(" :::backupDataInsertQuery:: "+backupDataInsertQuery+":: Inside method");
		    insertcount = backupServiceDaoImpl.insertBackupTable(backupDataInsertQuery);
		} 
		
		}  catch(Exception exception) {
			exceptionOccured = true;
			exception.printStackTrace();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: ");
		}
		return GlobalConstants.MSG_BACKUPSERVICE_DEPERSONALIZETABLE_DATA;
	}

	
}