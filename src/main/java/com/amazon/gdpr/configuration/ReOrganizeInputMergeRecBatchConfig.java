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

import com.amazon.gdpr.batch.ReorganizeInputMergeRecCompletionListener;
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
public class ReOrganizeInputMergeRecBatchConfig {
	
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_REORGANIZEINPUT_MERGEREC_BATCHCONFIG;
			
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
	public JdbcCursorItemReader<GdprDepersonalizationInput> gdprMergeRecDepersonalizationDBreader(@Value("#{jobParameters[RunId]}") long runId,
			@Value("#{jobParameters[StartDate]}") Date moduleStartDateTime,@Value("#{jobParameters[RecordType]}") String recordType
			) {
					
		String CURRENT_METHOD = "reader";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		String gdprDepersonalizationDataFetch = "";
		if(GlobalConstants.JOB_INPUT_CAN_RECORD.equalsIgnoreCase(recordType)) {
			gdprDepersonalizationDataFetch =  "SELECT DISTINCT GD.CANDIDATE__C, GD.BGC_APPLICATION__c,GD.CANDIDATE_SFDC_ID__C, GD.CATEGORY__C, GD.COUNTRY_CODE__C, "
				+ "'CANDIDATE' RECORD_TYPE, PH_AMAZON_ASSESSMENT_STATUS__C,PH_CANDIDATE_PROVIDED_STATUS__C, PH_MASTER_DATA_STATUS__C "
				+ "FROM SF_COPY.GDPR_DEPERSONALIZATION__C GD, GDPR.DATA_LOAD DL, GDPR.RUN_MGMT RM "
				+ "WHERE (GD.CREATEDDATE >= DL.LAST_DATA_LOADED_DATE OR GD.LASTMODIFIEDDATE >= DL.LAST_DATA_LOADED_DATE) "
				+ "AND GD.CREATEDDATE < RM.DATA_LOAD_DATE AND GD.CANDIDATE__C IS NULL AND GD.CANDIDATE_SFDC_ID__C IS NOT NULL AND BGC_STATUS__C IS NULL AND RM.RUN_ID = "+runId 
				+ " ORDER BY GD.CANDIDATE__C";
		} else {
			gdprDepersonalizationDataFetch = "SELECT DISTINCT GD.CANDIDATE__C, GD.CANDIDATE_SFDC_ID__C,GD.COUNTRY_CODE__C, 'EMPLOYEE' RECORD_TYPE, "
				+ "STAFF_EXP_STATUS__C, MASTER_DATA_STATUS__C "
				+ "FROM SF_COPY.GDPR_EMPLOYEE_DEPERSONALIZATION__C GD, GDPR.DATA_LOAD DL, GDPR.RUN_MGMT RM "
				+ "WHERE  (GD.CREATEDDATE >= DL.LAST_DATA_LOADED_DATE OR GD.LASTMODIFIEDDATE >= DL.LAST_DATA_LOADED_DATE) "
				+ "AND GD.CREATEDDATE < RM.DATA_LOAD_DATE AND GD.CANDIDATE__C IS NULL AND GD.CANDIDATE_SFDC_ID__C IS NOT NULL AND RM.RUN_ID = "+runId 
				+ " ORDER BY GD.CANDIDATE__C";
		}
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: GDPR Merge Depersonalization Data Fetch Query : "+gdprDepersonalizationDataFetch); 
		JdbcCursorItemReader<GdprDepersonalizationInput> reader = new JdbcCursorItemReader<GdprDepersonalizationInput>();
		reader.setDataSource(dataSource);
		reader.setSql(gdprDepersonalizationDataFetch);
		reader.setRowMapper(new GdprDepersonalizationInputMergeRecRowMapper());
		return reader;
	}
	
	//To set values into GdprDepersonalizationInput Object
	public class GdprDepersonalizationInputMergeRecRowMapper implements RowMapper<GdprDepersonalizationInput> {
		@SuppressWarnings("unused")
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_GDPRDEPERSONALIZATIONINPUTMERGERECROWMAPPER;
		
		@Override
		public GdprDepersonalizationInput mapRow(ResultSet rs, int rowNum) throws SQLException {			
			@SuppressWarnings("unused")
			String CURRENT_METHOD = "mapRow";
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
			
			String candidateId = rs.getString("CANDIDATE_SFDC_ID__C"); 
			String candidateOrApplicationId = (candidateId != null && ! (GlobalConstants.EMPTY_STRING.equalsIgnoreCase(candidateId.trim()))) ? 
					candidateId.trim() : rs.getString("BGC_Application__c");
					
			String recordType = rs.getString("RECORD_TYPE");
			if(GlobalConstants.JOB_INPUT_CAN_RECORD.equalsIgnoreCase(recordType)) {
				return new GdprDepersonalizationInput(
					candidateOrApplicationId, rs.getString("CATEGORY__C"), rs.getString("COUNTRY_CODE__C"), recordType, 
					rs.getString("PH_AMAZON_ASSESSMENT_STATUS__C"), rs.getString("PH_CANDIDATE_PROVIDED_STATUS__C"), 
					rs.getString("PH_MASTER_DATA_STATUS__C"));
			} else {
				return new GdprDepersonalizationInput(candidateId,  rs.getString("COUNTRY_CODE__C"), recordType,
						rs.getString("MASTER_DATA_STATUS__C"), rs.getString("STAFF_EXP_STATUS__C"));				
			}			
		}
	}
	
	//@Scope(value = "step")
	public class ReorganizeMergeRecDataProcessor implements ItemProcessor<GdprDepersonalizationInput, List<GdprDepersonalizationOutput>>{
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_JOB_REORGANIZE_MERGEREC_DATAPROCESSOR;
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
	
				moduleStartDateTime = jobParameters.getDate(GlobalConstants.JOB_INPUT_START_DATE);
		
				
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
							GlobalConstants.SUB_MODULE_REORGANIZE_MERGEREC_DATA, GlobalConstants.STATUS_FAILURE, moduleStartDateTime,  
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
					GdprDepersonalizationOutput gdprDepersonalizationOutput = new GdprDepersonalizationOutput(runId,
						gdprDepersonalizationInput.getCandidate(), Integer.parseInt(mapFieldCategory.get(fieldCategory)), 
						gdprDepersonalizationInput.getCountryCode(), fieldValue, 
						GlobalConstants.STATUS_SCHEDULED);
					lstGdprDepersonalizationOutput.add(gdprDepersonalizationOutput);
				
			}
			System.out.println("lstGdprDepersonalizationOutput Merge::::"+lstGdprDepersonalizationOutput);
			hvhOutputDaoImpl.batchInsertGdprDepersonalizationOutput(lstGdprDepersonalizationOutput);
			return lstGdprDepersonalizationOutput;
		}
	}
				
	@Bean
	public Step reorganizeInputMergeRecStep() {
		String CURRENT_METHOD = "reorganizeInputMergeRecStep";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		
		Step step = null;
		step = stepBuilderFactory.get("reorganizeInputMergeRecStep")
			.<GdprDepersonalizationInput, List<GdprDepersonalizationOutput>> chunk(SqlQueriesConstant.BATCH_ROW_COUNT)
			.reader(gdprMergeRecDepersonalizationDBreader(0, new Date(),""))
			.processor(new ReorganizeMergeRecDataProcessor())
			.build();
		return step;		
	}
	
	@Bean
	public Job processReorganizeInputMergeRecJob() {
		String CURRENT_METHOD = "processReorganizeInputMergeRecJob";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before Batch Process : "+LocalTime.now());
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		Job job = null;
		Boolean exceptionOccured = false;
		String reOrganizeData = CURRENT_CLASS +":::"+CURRENT_METHOD+"::";
		String errorDetails = "";
		
		job = jobBuilderFactory.get(CURRENT_METHOD)
				.incrementer(new RunIdIncrementer()).listener(reorganizeInputMergeReclistener(GlobalConstants.JOB_REORGANIZE_INPUT_MERGEREC_PROCESSOR))										
				.flow(reorganizeInputMergeRecStep())
				.end()
				.build();
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After Batch Process : "+LocalTime.now());
		return job;
	}

	@Bean
	public JobExecutionListener reorganizeInputMergeReclistener(String jobRelatedName) {
		String CURRENT_METHOD = "reorganizeInputMergeReclistener";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Job Completion listener : "+LocalTime.now());
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		return new ReorganizeInputMergeRecCompletionListener(jobRelatedName);
	}	
}