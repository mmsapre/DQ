package com.dq.utils;

public class FAEDataTypeValidator {

	private String dataType;
	private static String BOOLEAN_TRUE = "true";
	private static String BOOLEAN_FALSE = "false";

	public boolean isValidDataType(String value) {
		if (dataType.equalsIgnoreCase("int"))
			return isInt(value.toString());

		else if (dataType.equalsIgnoreCase("long"))
			return isLong(value.toString());

		else if (dataType.equalsIgnoreCase("float"))
			return isFloat(value.toString());

		else if (dataType.equalsIgnoreCase("double"))
			return isDouble(value.toString());

		else if (dataType.equalsIgnoreCase("boolean"))
			return isBoolean(value.toString());

		else if (dataType.equalsIgnoreCase("string"))
			return isString(value.toString());
		return false;
	}

	boolean isBoolean(String str) {
		try {
			if (str.trim().equalsIgnoreCase(BOOLEAN_TRUE)
					|| str.trim().equalsIgnoreCase(BOOLEAN_FALSE))
				return true;
			else
				return false;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	boolean isLong(String str) {
		try {
			Long.parseLong(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	boolean isInt(String str) {
		try {
			Integer.parseInt(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	boolean isDouble(String str) {
		try {
			Double.parseDouble(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	boolean isFloat(String str) {
		try {
			Float.parseFloat(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	boolean isString(String str) {
		return str.matches(".*[a-zA-Z]+.*");
	}
}
