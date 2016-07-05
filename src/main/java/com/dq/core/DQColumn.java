package com.dq.core;

import java.util.ArrayList;
import java.util.List;

import com.dq.mappings.Column;

public class DQColumn {
	
	private String name;
	private String dataType;
	private List<DQValidations> columnConstraint=new ArrayList<DQValidations>();
	
	public DQColumn(String aName,String aDataType,List<DQValidations> aValidator){
		this.name=aName;
		this.dataType=aDataType;
		this.columnConstraint=aValidator;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public List<DQValidations> getColumnConstraint() {
		return columnConstraint;
	}

	public void setColumnConstraint(List<DQValidations> columnConstraint) {
		this.columnConstraint = columnConstraint;
	}


}