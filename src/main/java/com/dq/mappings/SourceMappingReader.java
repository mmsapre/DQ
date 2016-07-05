package com.dq.mappings;

import java.io.File;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

public class SourceMappingReader {

	//private static JAXBContext jaxbContext = null;

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

	public Source sourceMappingLoader(String aSrcMapPath) {
		Source lSrcMapXml = null;
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(Source.class);
			Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
			lSrcMapXml = (Source) unmarshaller.unmarshal(new File(
					aSrcMapPath));
		
		} catch (JAXBException e) {
			e.printStackTrace();
		}catch(Error er){
			er.printStackTrace();
		}
		return lSrcMapXml;
	}
	
	public List<Table> getTableFromSource(Source aSrc) {
		List<Table> lTblst = null;
		lTblst = aSrc.getTable();
		return lTblst;
	}

}
