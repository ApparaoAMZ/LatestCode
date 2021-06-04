package com.amazon.gdpr.model;

public class GdprDepersonalizationInput {
	
	public String candidate;
	public String category;
	public String countryCode;
	public String recordType;
	
	public String bgcStatus;
	public String amazonAssessmentStatus;
	public String candidateProvidedStatus;
	public String masterDataStatus;
	public String withConsentStatus;
	
	public String masterData;
	public String staffExperience;
	public String biographicalPhotographs;
	
	public GdprDepersonalizationInput() {
		
	}
	
	/**
	 * @param candidate
	 * @param category
	 * @param countryCode
	 * @param recordType
	 * @param bgcStatus
	 * @param amazonAssessmentStatus
	 * @param candidateProvidedStatus
	 * @param masterDataStatus
	 * @param withConsentStatus
	 */
	public GdprDepersonalizationInput(String candidate, String category, String countryCode, String recordType, String bgcStatus,
			String amazonAssessmentStatus, String candidateProvidedStatus, String masterDataStatus,
			String withConsentStatus) {
		super();
		this.candidate = candidate;
		this.category = category;
		this.countryCode = countryCode;
		this.recordType = recordType;
		this.bgcStatus = bgcStatus;
		this.amazonAssessmentStatus = amazonAssessmentStatus;
		this.candidateProvidedStatus = candidateProvidedStatus;
		this.masterDataStatus = masterDataStatus;
		this.withConsentStatus = withConsentStatus;
	}
	
	public GdprDepersonalizationInput(String candidate, String category, String countryCode, String recordType,
			String amazonAssessmentStatus, String candidateProvidedStatus, String masterDataStatus) {
		super();
		this.candidate = candidate;
		this.category = category;
		this.countryCode = countryCode;
		this.recordType = recordType;
		this.amazonAssessmentStatus = amazonAssessmentStatus;
		this.candidateProvidedStatus = candidateProvidedStatus;
		this.masterDataStatus = masterDataStatus;
		
	}
	public GdprDepersonalizationInput(String candidate, String countryCode, String recordType, 
			String masterData, String staffExperience, String biographicalPhotographs) {
		super();
		this.candidate = candidate;
		this.countryCode = countryCode;
		this.recordType = recordType;
		this.masterData = masterData;
		this.staffExperience = staffExperience;
		this.biographicalPhotographs = biographicalPhotographs;
	}
	
	public GdprDepersonalizationInput(String candidate, String countryCode, String recordType, 
			String masterData, String staffExperience) {
		super();
		this.candidate = candidate;
		this.countryCode = countryCode;
		this.recordType = recordType;
		this.masterData = masterData;
		this.staffExperience = staffExperience;
		
	}
	
	
	/**
	 * @return the candidate
	 */
	public String getCandidate() {
		return candidate;
	}
	/**
	 * @param candidate the candidate to set
	 */
	public void setCandidate(String candidate) {
		this.candidate = candidate;
	}
	/**
	 * @return the category
	 */
	public String getCategory() {
		return category;
	}
	/**
	 * @param category the category to set
	 */
	public void setCategory(String category) {
		this.category = category;
	}
	/**
	 * @return the countryCode
	 */
	public String getCountryCode() {
		return countryCode;
	}
	/**
	 * @param countryCode the countryCode to set
	 */
	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}
	/**
	 * @return the recordType
	 */
	public String getRecordType() {
		return recordType;
	}

	/**
	 * @param recordType the recordType to set
	 */
	public void setRecordType(String recordType) {
		this.recordType = recordType;
	}
	/**
	 * @return the bgcStatus
	 */
	public String getBgcStatus() {
		return bgcStatus;
	}
	/**
	 * @param bgcStatus the bgcStatus to set
	 */
	public void setBgcStatus(String bgcStatus) {
		this.bgcStatus = bgcStatus;
	}
	/**
	 * @return the amazonAssessmentStatus
	 */
	public String getAmazonAssessmentStatus() {
		return amazonAssessmentStatus;
	}
	/**
	 * @param amazonAssessmentStatus the amazonAssessmentStatus to set
	 */
	public void setAmazonAssessmentStatus(String amazonAssessmentStatus) {
		this.amazonAssessmentStatus = amazonAssessmentStatus;
	}
	/**
	 * @return the candidateProvidedStatus
	 */
	public String getCandidateProvidedStatus() {
		return candidateProvidedStatus;
	}
	/**
	 * @param candidateProvidedStatus the candidateProvidedStatus to set
	 */
	public void setCandidateProvidedStatus(String candidateProvidedStatus) {
		this.candidateProvidedStatus = candidateProvidedStatus;
	}
	/**
	 * @return the masterDataStatus
	 */
	public String getMasterDataStatus() {
		return masterDataStatus;
	}
	/**
	 * @param masterDataStatus the masterDataStatus to set
	 */
	public void setMasterDataStatus(String masterDataStatus) {
		this.masterDataStatus = masterDataStatus;
	}
	/**
	 * @return the withConsentStatus
	 */
	public String getWithConsentStatus() {
		return withConsentStatus;
	}
	/**
	 * @param withConsentStatus the withConsentStatus to set
	 */
	public void setWithConsentStatus(String withConsentStatus) {
		this.withConsentStatus = withConsentStatus;
	}
	/**
	 * @return the masterData
	 */
	public String getMasterData() {
		return masterData;
	}

	/**
	 * @param masterData the masterData to set
	 */
	public void setMasterData(String masterData) {
		this.masterData = masterData;
	}

	/**
	 * @return the staffExperience
	 */
	public String getStaffExperience() {
		return staffExperience;
	}

	/**
	 * @param staffExperience the staffExperience to set
	 */
	public void setStaffExperience(String staffExperience) {
		this.staffExperience = staffExperience;
	}

	/**
	 * @return the biographicalPhotographs
	 */
	public String getBiographicalPhotographs() {
		return biographicalPhotographs;
	}

	/**
	 * @param biographicalPhotographs the biographicalPhotographs to set
	 */
	public void setBiographicalPhotographs(String biographicalPhotographs) {
		this.biographicalPhotographs = biographicalPhotographs;
	}
}