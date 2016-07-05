package com.dq.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class DataCheck implements Serializable {

	private static Logger LOGGER = Logger.getLogger(DataCheck.class);
	private List<String> cColumns = new ArrayList<String>();
	private String cDelimiter;
	private boolean isValid;
	private List<String> cBadRowDescription;
	private String cActualValue;
	private String cFormatValue;

	

	public DataCheck(String value, String delimiter) {
		this.cActualValue = value;
		this.cDelimiter = delimiter;
		
		if (cActualValue != null) {
			cActualValue=cActualValue.replaceAll(delimiter, ",");
			LOGGER.info("Delimit ="+ delimiter+":::");
            String[] tokens = cActualValue.split(",");
            for (int i = 0; i < tokens.length; i++) {
            	LOGGER.info("DataCheck = " + tokens[i].toString());
            	cColumns.add(tokens[i]);
            }
            LOGGER.info("cActualValue = " + cActualValue.toString());
        }
		cBadRowDescription = new ArrayList<String>();
		cFormatValue=cActualValue.replaceAll(",", "\t");
		LOGGER.info("cFormatValue = " + cFormatValue.toString());
	}

	public String getcFormatValue() {
		return cFormatValue;
	}

	public void setcFormatValue(String cFormatValue) {
		this.cFormatValue = cFormatValue;
	}
	
	public List<String> getcColumns() {
		return cColumns;
	}

	public void setcColumns(List<String> cColumns) {
		this.cColumns = cColumns;
	}

	public String getcDelimiter() {
		return cDelimiter;
	}

	public void setcDelimiter(String cDelimiter) {
		this.cDelimiter = cDelimiter;
	}

	public boolean isValid() {
		return isValid;
	}

	public void setValid(boolean isValid) {
		this.isValid = isValid;
	}

	public List<String> getcBadRowDescription() {
		return cBadRowDescription;
	}

	public void setcBadRowDescription(List<String> cBadRowDescription) {
		this.cBadRowDescription = cBadRowDescription;
	}

	public String getcActualValue() {
		return cActualValue;
	}

	public void setcActualValue(String cActualValue) {
		this.cActualValue = cActualValue;
	}
	
	public String toString(){
		StringBuffer dataOut=new StringBuffer();
		dataOut.append(cFormatValue);
		dataOut.append(convertError());
		return dataOut.toString();
	}
	
	private String convertError(){
		StringBuffer strBuffErrDesc=new StringBuffer();
		int i=0;
		for(String strErrDesc :cBadRowDescription){
			strBuffErrDesc.append("\t");
			strBuffErrDesc.append(strErrDesc);
			if(i>0)
				strBuffErrDesc.append("|");
		}
		return strBuffErrDesc.toString();
	}

}
