package com.dq.core;

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class RDDMultipleBooleanDataFormat <Boolean> extends
MultipleTextOutputFormat<Boolean, DataCheck>{
	
	@Override
	protected Boolean generateActualKey(Boolean key, DataCheck value) {
		return null;
	}
	@Override
	protected String generateFileNameForKeyValue(Boolean key, DataCheck value,
			String name) {
		if ("true".equalsIgnoreCase(key.toString()))
			return "dq-pass";
		else
			return "dq-fail";

	}

}
