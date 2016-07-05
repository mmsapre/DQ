package com.dq.core;

import org.apache.spark.sql.api.java.UDF1;

import com.dq.utils.FAEError;

public class ErrorDescription implements UDF1<String, String> {

	@Override
	public String call(String paramT1) throws Exception {
		// TODO Auto-generated method stub
		return FAEError.DATA_FORMAT_ERROR.getErrorDescription();
	}

}
