package com.amazon.gdpr.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.amazon.gdpr.model.gdpr.input.Country;
import com.amazon.gdpr.model.gdpr.input.GdprHerokuTagging;
import com.amazon.gdpr.model.gdpr.input.ImpactTable;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * The DAOImpl file fetches the JDBCTemplate created in the DatabaseConfig and 
 * Connects with the schema to fetch the Input table rows 
 ****************************************************************************************/
@Transactional
@Repository
public class GdprInputFetchDaoImpl {
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_GDPRINPUTFETCHDAOIMPL;
	
	@Autowired
	@Qualifier("gdprJdbcTemplate")
	private JdbcTemplate jdbcTemplate;
	
	/**
	 * Fetches the Country Table rows
	 * @return List of Country
	 */
	public List<Country> fetchCountry(List<String> selectedCountries){
		String CURRENT_METHOD = "fetchCountry";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		
		String inSql = String.join(",", Collections.nCopies(selectedCountries.size(), "?"));
		@SuppressWarnings("unchecked")
		List<Country> lstCountry = jdbcTemplate.query(String.format(SqlQueriesConstant.COUNTRY_FETCH, inSql), 
				selectedCountries.toArray(), new CountryRowMapper());
		return lstCountry;
	}
	
	/**
	 * Fetches all the Country Table rows
	 * @return List of Country
	 */
	public List<Country> fetchAllCountries(){
		String CURRENT_METHOD = "fetchAllCountries";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		@SuppressWarnings("unchecked")
		List<Country> lstCountry = jdbcTemplate.query(SqlQueriesConstant.ALL_COUNTRY_FETCH, new CountryRowMapper());
		return lstCountry;
	}
	/****************************************************************************************
	 * This rowMapper converts the row data from COUNTRY table to Country Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class CountryRowMapper implements RowMapper{
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_COUNTRYROWMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public Country mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
									
			return new Country(rs.getString("REGION"), rs.getString("COUNTRY_CODE"));
		}
	}
	
	/**
	 * Fetches the ImpactTable Table rows
	 * @return List of ImpactTable
	 */
	public List<ImpactTable> fetchImpactTable(){
		String CURRENT_METHOD = "fetchImpactTable";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		@SuppressWarnings("unchecked")
		List<ImpactTable> lstImpactTable = jdbcTemplate.query(SqlQueriesConstant.IMPACT_TABLE_FETCH_ALL, new ImpactTableRowMapper());
		return lstImpactTable;
	}
	
	/****************************************************************************************
	 * This rowMapper converts the row data from IMPACTTABLE table to ImpactTable Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class ImpactTableRowMapper implements RowMapper{
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_IMPACTTABLEROWMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public ImpactTable mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
			
			return new ImpactTable(rs.getInt("IMPACT_TABLE_ID"), rs.getString("IMPACT_SCHEMA"), rs.getString("IMPACT_TABLE_NAME"),  
					rs.getString("IMPACT_COLUMNS"), rs.getString("IMPACT_TABLE_TYPE"), rs.getString("PARENT_SCHEMA"), 
					rs.getString("PARENT_TABLE_NAME"), rs.getString("IMPACT_TABLE_COLUMN"), rs.getString("PARENT_TABLE_COLUMN"));
		}
	}
	
	/**
	 * Fetches the GDPR_HEROKU_TAGGING Table rows
	 * @return List of GdprHerokuTagging
	 */
	public List<GdprHerokuTagging> fetchGdprHerokuTagging(){
		String CURRENT_METHOD = "fetchGdprHerokuTagging";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		@SuppressWarnings("unchecked")
		List<GdprHerokuTagging> lstGdprHerokuTagging = jdbcTemplate.query(SqlQueriesConstant.GDPR_HEROKU_TAGGING_FETCH, new GdprHerokuTaggingRowMapper());
		return lstGdprHerokuTagging;
	}
	
	/****************************************************************************************
	 * This rowMapper converts the row data from GDPR_HEROKU_TAGGING table to GdprHerokuTagging Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class GdprHerokuTaggingRowMapper implements RowMapper{
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_GDPRHEROKUTAGGING_ROWMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public GdprHerokuTagging mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
			
			return new GdprHerokuTagging(rs.getString("NAME"),rs.getString("CATEGORY__C"),rs.getLong("COUNT__C"),rs.getString("ID_SPACE__C"),rs.getString("HEROKU_STATUS__C"),rs.getTimestamp("CREATEDDATE"));
		}
	}
	
	/**
	 * Fetches the GDPR_HEROKU_TAGGING Table rows
	 * @return List of GdprHerokuTagging
	 */
	public List<String> fetchCountries(long runId){
		String CURRENT_METHOD = "fetchCountries";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		@SuppressWarnings("unchecked")
		List<String> lstCountries = jdbcTemplate.query(SqlQueriesConstant.GDPR_COUNTRIES_FETCH, new Object[]{runId}, new GdprCountriesFetch());
		return lstCountries;
	}
	
	/**
	 * Fetches the GDPR_HEROKU_TAGGING Table rows
	 * @return List of GdprHerokuTagging
	 */
	public List<String> fetchEmpCountries(long runId){
		String CURRENT_METHOD = "fetchEmpCountries";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		@SuppressWarnings("unchecked")
		List<String> lstCountries = jdbcTemplate.query(SqlQueriesConstant.GDPR_EMPLOYEE_COUNTRIES_FETCH, new Object[]{runId}, new GdprCountriesFetch());
		return lstCountries;
	}
	
	
	/****************************************************************************************
	 * This rowMapper converts the row data from GDPR_HEROKU_TAGGING table to GdprHerokuTagging Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class GdprCountriesFetch implements RowMapper{
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_GDPR_COUNTRIES_FETCH;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public String mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
			
			return rs.getString(1);
		}
	}
	
	
}