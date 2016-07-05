package com.dq.mappings1;

import java.io.File;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

public class SourceReader {

	public static void main(String[] args) throws Exception {
		JAXBContext jc = JAXBContext.newInstance(Source.class);

		Unmarshaller unmarshaller = jc.createUnmarshaller();
		Source srcMapXml = (Source) unmarshaller
				.unmarshal(new File(
						"E:/Project/Workspace/mapping/src/main/resources/sha_oncology.xml"));
		List<Table> tblMap = srcMapXml.getTable();
		//List<Column> colMapLst = tblMap.getColumn();
//		for (Column colMap : colMapLst) {
//			List<String> lstValString = colMap.getValidation();
//			for (String strValidations : lstValString) {
//				System.out.println("Validations :::" + strValidations);
//			}
//		}
	}
}
