package com.amazon.gdpr.model.gdpr.input;

import java.sql.Timestamp;

public class GdprHerokuTagging {
	String category;
	long count;
	String iDSpace;
	String herokuStatus;
	String name;
	Timestamp createddate;
	
  public GdprHerokuTagging(){
		
	}
  
  public GdprHerokuTagging(String name, String category, long count, String iDSpace,
		  String herokuStatus, Timestamp createddate) {
		super();		
		this.name = name;
		this.category = category;
		this.count = count;
		this.iDSpace = iDSpace;
		this.herokuStatus = herokuStatus;
		this.createddate = createddate;
		
	}

public String getCategory() {
	return category;
}

public void setCategory(String category) {
	this.category = category;
}

public long getCount() {
	return count;
}

public void setCount(long count) {
	this.count = count;
}

public String getiDSpace() {
	return iDSpace;
}

public void setiDSpace(String iDSpace) {
	this.iDSpace = iDSpace;
}

public String getHerokuStatus() {
	return herokuStatus;
}

public void setHerokuStatus(String herokuStatus) {
	this.herokuStatus = herokuStatus;
}

public String getName() {
	return name;
}

public void setName(String name) {
	this.name = name;
}

public Timestamp getCreateddate() {
	return createddate;
}

public void setCreateddate(Timestamp createddate) {
	this.createddate = createddate;
}
  
  
}
