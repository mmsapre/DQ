package com.dq.utils;

public enum FAEError {

	FILE_NOT_FOUND(0, "File Not Found."), DATA_FORMAT_ERROR(1,
			"Tuple does not match the definition."),NON_UNIQUE_CONSTRAINT(2,"Column is not matching unique constraint."),NOT_NULL_CONSTRAINT(3,"Column cannot be null.");

	private int code;
	private String errorDescription;

	FAEError(int aCode, String aDescription) {
		this.code = aCode;
		this.errorDescription = aDescription;
	}

	public String getErrorDescription() {
		return errorDescription;
	}

	public void setErrorDescription(String errorDescription) {
		this.errorDescription = errorDescription;
	}

	public String toString() {
		return errorDescription;
	}
}
