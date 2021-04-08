package com.amazon.gdpr.configuration;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.amazon.gdpr.batch.ReorganizeInputEmpCompletionListener;
import com.amazon.gdpr.dao.GdprInputDaoImpl;
import com.amazon.gdpr.dao.HvhOutputDaoImpl;
import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.model.GdprDepersonalizationInput;
import com.amazon.gdpr.model.GdprDepersonalizationOutput;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * This Configuration handles the Reading of SALESFORCE.GDPR_DEPERSONALIZATION__C table 
 * and Writing into GDPR.GDPR_DEPERSONALIZATION
 ****************************************************************************************/
@EnableScheduling
@EnableBatchProcessing
@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
@Configuration
public class ReOrganizeInputEmpBatchConfig {
	
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_REORGANIZEINPUT_EMP_BATCHCONFIG;
			
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public DataSource dataSource;

	@Autowired
	public HvhOutputDaoImpl hvhOutputDaoImpl; 
		
	@Autowired
	GdprInputDaoImpl gdprInputDaoImpl;
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	public long runId;
	public Date moduleStartDateTime;
	
	@Bean
	@StepScope
	public JdbcCursorItemReader<GdprDepersonalizationInput> gdprEmpDepersonalizationDBreader(@Value("#{jobParameters[RunId]}") long runId,
			@Value("#{jobParameters[StartDate]}") Date moduleStartDateTime, @Value("#{jobParameters[CountryCode]}") String countryCode, 
			@Value("#{jobParameters[RecordType]}") String recordType) {
					
		String CURRENT_METHOD = "reader";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		String gdprDepersonalizationDataFetch = "";
		
		gdprDepersonalizationDataFetch = "SELECT DISTINCT GD.CANDIDATE__C, GD.COUNTRY_CODE__C, 'EMPLOYEE' RECORD_TYPE, "
			+ "CASE WHEN (GD.BIO_PHOTO_PROCESSED_DATE__C IS NOT NULL AND GD.BIO_PHOTO_PROCESSED_DATE__C >= DL.LAST_DATA_LOADED_DATE AND " 
			+ "GD.BIO_PHOTO_PROCESSED_DATE__C < RM.DATA_LOAD_DATE) THEN GD.BIO_PHOTO_STATUS__C ELSE 'NOT TO BE PROCESSED' END BIO_PHOTO_STATUS__C, " 
			+ "CASE WHEN (GD.STAFF_EXP_PROCESSED_DATE__C IS NOT NULL AND GD.STAFF_EXP_PROCESSED_DATE__C >= DL.LAST_DATA_LOADED_DATE AND " 
			+ "GD.STAFF_EXP_PROCESSED_DATE__C < RM.DATA_LOAD_DATE) THEN GD.STAFF_EXP_STATUS__C ELSE 'NOT TO BE PROCESSED' END STAFF_EXP_STATUS__C, " 
			+ "CASE WHEN (GD.MASTER_DATA_PROCESSED_DATE__C IS NOT NULL AND GD.MASTER_DATA_PROCESSED_DATE__C >= DL.LAST_DATA_LOADED_DATE AND "
			+ "GD.MASTER_DATA_PROCESSED_DATE__C < RM.DATA_LOAD_DATE) THEN GD.MASTER_DATA_STATUS__C ELSE 'NOT TO BE PROCESSED' END MASTER_DATA_STATUS__C "
			+ "FROM SF_COPY.GDPR_EMPLOYEE_DEPERSONALIZATION__C GD, GDPR.DATA_LOAD DL, GDPR.RUN_MGMT RM "
			+ "WHERE (GD.BIO_PHOTO_STATUS__C IN ('DEPERSONALIZATION_COMPLETE', 'DEPERSON_NO_TASK_COMPLETE') "
			+ "OR GD.STAFF_EXP_STATUS__C IN ('DEPERSONALIZATION_COMPLETE', 'DEPERSON_NO_TASK_COMPLETE') " 
			+ "OR GD.MASTER_DATA_STATUS__C IN ('DEPERSONALIZATION_COMPLETE', 'DEPERSON_NO_TASK_COMPLETE')) AND GD.COUNTRY_CODE__C = DL.COUNTRY_CODE "
			+ "AND (GD.CREATEDDATE >= DL.LAST_DATA_LOADED_DATE OR GD.LASTMODIFIEDDATE >= DL.LAST_DATA_LOADED_DATE) "
			+ "AND GD.CREATEDDATE < RM.DATA_LOAD_DATE AND RM.RUN_ID = "+runId 
			+ " AND GD.COUNTRY_CODE__C =  \'"+countryCode+"\' ORDER BY GD.CANDIDATE__C";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: GDPR Emp Depersonalization Data Fetch Query : "+gdprDepersonalizationDataFetch); 
		JdbcCursorItemReader<GdprDepersonalizationInput> reader = new JdbcCursorItemReader<GdprDepersonalizationInput>();
		reader.setDataSource(dataSource);
		reader.setSql(gdprDepersonalizationDataFetch);
		reader.setRowMapper(new GdprDepersonalizationInputEmpRowMapper());
		return reader;
	}
	
	//To set values into GdprDepersonalizationInput Object
	public class GdprDepersonalizationInputEmpRowMapper implements RowMapper<GdprDepersonalizationInput> {
		@SuppressWarnings("unused")
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_GDPRDEPERSONALIZATIONINPUTEMPROWMAPPER;
		
		@Override
		public GdprDepersonalizationInput mapRow(ResultSet rs, int rowNum) throws SQLException {			
			@SuppressWarnings("unused")
			String CURRENT_METHOD = "mapRow";
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
			
			String candidateId = rs.getString("CANDIDATE__C"); 					
			String recordType = rs.getString("RECORD_TYPE");
			
			return new GdprDepersonalizationInput(candidateId, rs.getString("COUNTRY_CODE__C"), recordType,
					rs.getString("MASTER_DATA_STATUS__C"), rs.getString("STAFF_EXP_STATUS__C"), rs.getString("BIO_PHOTO_STATUS__C"));				
		}
	}
	
	//@Scope(value = "step")
	public class ReorganizeEmpDataProcessor implements ItemProcessor<GdprDepersonalizationInput, List<GdprDepersonalizationOutput>>{
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_JOB_REORGANIZE_EMP_DATAPROCESSOR;
		private Map<String, String> mapCategory = null;
		private Map<String, String> mapFieldCategory = null;
		
		@BeforeStep
		public void beforeStep(final StepExecution stepExecution) throws GdprException{
			String CURRENT_METHOD = "beforeStep";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Job Before Step : "+LocalTime.now());
			Boolean exceptionOccured = false;
			String reOrganizeData = CURRENT_CLASS +":::"+CURRENT_METHOD+"::";
			String errorDetails = "";
						
			try {				
				JobParameters jobParameters = stepExecution.getJobParameters();
				runId	= jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_ID);
				long currentRun 	= jobParameters.getLong(GlobalConstants.JOB_INPUT_JOB_ID);
				String countryCode = jobParameters.getString(GlobalConstants.JOB_INPUT_COUNTRY_CODE);
				moduleStartDateTime = jobParameters.getDate(GlobalConstants.JOB_INPUT_START_DATE);
				String recordType = jobParameters.getString(GlobalConstants.JOB_INPUT_RECORDTYPE);
				
				mapCategory = gdprInputDaoImpl.fetchCategoryDetails();
				mapFieldCategory = gdprInputDaoImpl.getMapFieldCategory();
			} catch (Exception exception) {
				exceptionOccured = true;
				reOrganizeData  = reOrganizeData + exception.getMessage() ;				
				errorDetails = Arrays.toString(exception.getStackTrace());
			}
			try {
				if(exceptionOccured){
					RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, 
							GlobalConstants.SUB_MODULE_REORGANIZE_EMP_DATA, GlobalConstants.STATUS_FAILURE, moduleStartDateTime,  
							new Date(), reOrganizeData, errorDetails);
					moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
					throw new GdprException(reOrganizeData, errorDetails);
				}
			} catch(GdprException exception) {
				reOrganizeData = reOrganizeData + GlobalConstants.ERR_MODULE_MGMT_INSERT;
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeData);
				errorDetails = errorDetails + exception.getExceptionDetail();
				throw new GdprException(reOrganizeData, errorDetails); 
			}
		}
				
		@Override
		public List<GdprDepersonalizationOutput> process(GdprDepersonalizationInput gdprDepersonalizationInput) throws NoSuchFieldException, 
			SecurityException, IllegalArgumentException, IllegalAccessException {
			String CURRENT_METHOD = "process";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
			Boolean exceptionOccured = false;
			String reOrganizeData = CURRENT_CLASS +":::"+CURRENT_METHOD+"::";
			List<GdprDepersonalizationOutput> lstGdprDepersonalizationOutput = new ArrayList<GdprDepersonalizationOutput>();
			String errorDetails = "";
			
			if(mapCategory == null)	
				mapCategory = gdprInputDaoImpl.fetchCategoryDetails();
			if(mapFieldCategory == null)
				mapFieldCategory = gdprInputDaoImpl.getMapFieldCategory();
			List<String> fieldCategoryList = new ArrayList<String>(mapFieldCategory.keySet());				
			for(String fieldCategory : fieldCategoryList){					
				Field field = GdprDepersonalizationInput.class.getDeclaredField(fieldCategory);
				String fieldValue = (String) field.get(gdprDepersonalizationInput);
				if(GlobalConstants.STATUS_DEPERSONALIZATION_COMPLETE.equalsIgnoreCase(fieldValue) || 
						GlobalConstants.STATUS_DEPERSON_NO_TASK_COMPLETE.equalsIgnoreCase(fieldValue)) {
					GdprDepersonalizationOutput gdprDepersonalizationOutput = new GdprDepersonalizationOutput(runId,
						gdprDepersonalizationInput.getCandidate(), Integer.parseInt(mapFieldCategory.get(fieldCategory)), 
						gdprDepersonalizationInput.getCountryCode(), fieldValue, 
						GlobalConstants.STATUS_SCHEDULED);
					lstGdprDepersonalizationOutput.add(gdprDepersonalizationOutput);
				}
			}
			hvhOutputDaoImpl.batchInsertGdprDepersonalizationOutput(lstGdprDepersonalizationOutput);
			return lstGdprDepersonalizationOutput;
		}
	}
				
	@Bean
	public Step reorganizeInputEmpStep() {
		String CURRENT_METHOD = "reorganizeInputEmpStep";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		
		Step step = null;
		step = stepBuilderFactory.get("reorganizeInputEmpStep")
			.<GdprDepersonalizationInput, List<GdprDepersonalizationOutput>> chunk(SqlQueriesConstant.BATCH_ROW_COUNT)
			.reader(gdprEmpDepersonalizationDBreader(0, new Date(), "", ""))
			.processor(new ReorganizeEmpDataProcessor())
			.build();
		return step;		
	}
	
	@Bean
	public Job processReorganizeInputEmpJob() {
		String CURRENT_METHOD = "processReorganizeInputEmpJob";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before Batch Process : "+LocalTime.now());
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		Job job = null;
		Boolean exceptionOccured = false;
		String reOrganizeData = CURRENT_CLASS +":::"+CURRENT_METHOD+"::";
		String errorDetails = "";
		
		job = jobBuilderFactory.get(CURRENT_METHOD)
				.incrementer(new RunIdIncrementer()).listener(reorganizeInputEmplistener(GlobalConstants.JOB_REORGANIZE_INPUT_EMP_PROCESSOR))										
				.flow(reorganizeInputEmpStep())
				.end()
				.build();
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After Batch Process : "+LocalTime.now());
		return job;
	}

	@Bean
	public JobExecutionListener reorganizeInputEmplistener(String jobRelatedName) {
		String CURRENT_METHOD = "reorganizeInputEmplistener";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Job Completion listener : "+LocalTime.now());
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		return new ReorganizeInputEmpCompletionListener(jobRelatedName);
	}	
}