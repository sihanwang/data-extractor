package com.thomsonreuters.ce.dbor.cfsdi.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.Date;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.thomsonreuters.ce.dbor.file.FileUtilities;
import com.thomsonreuters.ce.dbor.cfsdi.SDIConstants;
import com.thomsonreuters.ce.dbor.cfsdi.Starter;
import com.thomsonreuters.ce.dbor.cfsdi.cursor.CursorType;
import com.thomsonreuters.ce.dbor.cfsdi.cursor.SDICursorRow;
import com.thomsonreuters.ce.dbor.cfsdi.cursor.struct.OracleObject;
import com.thomsonreuters.ce.dbor.cfsdi.util.Counter;
import com.thomsonreuters.ce.queue.Pipe;


public abstract class GZIPSDIFileGenerator{

	
	private int ActiveNum=0;
	private int ThreadNum;
	private Pipe<HashMap<String,SDICursorRow[]>> InQ;	
	
	protected String Location;
	protected String FileName;
	
	protected Date CoverageStart;
	protected Date CoverageEnd;
	
	private BufferedWriter bw=null; 
	private Counter count;
		
	public GZIPSDIFileGenerator(String Loc,String FN, Date CS, Date CE ,int Thread_Num, Pipe<HashMap<String,SDICursorRow[]>> InPort, Counter ct)
	{
		this.Location=Loc;
		this.FileName=FN;
		this.CoverageStart=CS;
		this.CoverageEnd=CE;
		
		this.ThreadNum=Thread_Num;
		this.InQ=InPort;
		this.count=ct;
		
		this.count.Increase();
	}
	
	public boolean IsFullLoad()
	{
		if (CoverageStart==null)
		{
			return true;
		}
		else
		{
			return false;
		}		
	}	
	
	public Date getCoverageEnd() {
		return CoverageEnd;
	}

	public Date getCoverageStart() {
		return CoverageStart;
	}
	
	public String getAction(Date EntityCreateDate)
	{
		if (CoverageStart==null)
		{
			return "Insert";
		}
		else if (EntityCreateDate.after(CoverageStart))
		{
			return "Insert";
		}
		else
		{
			return "Overwrite";
		}
	}
	

	public final void Start() {
		

		
		File thisTempFile = new File(this.Location, this.FileName+".temp");
		
		//Delete temp file if it exists
		if (thisTempFile.exists() == true) {
			thisTempFile.delete();
		}			
		
		
		try {
			thisTempFile.createNewFile();

			GZIPOutputStream zfile = new GZIPOutputStream(new FileOutputStream(thisTempFile));
			bw = new BufferedWriter(new OutputStreamWriter(zfile, "UTF8"));

			bw.write(getHeader());
			bw.flush();

			Thread[] ThreadArray=new Thread[this.ThreadNum];
			for (int i = 0; i < this.ThreadNum; i++) {
				ThreadArray[i]=new Thread( new Worker());
			}		
			
			for (int i = 0; i < this.ThreadNum; i++) {
				ThreadArray[i].start();
			}

			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			Starter.cfLogger.error("FileNotFoundException",e);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			Starter.cfLogger.error("UnsupportedEncodingException",e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			Starter.cfLogger.error("IOException",e);
		} 
		
	}
	
	private void WriteContentItem(String CI)
	{
		synchronized (GZIPSDIFileGenerator.this) {
			try {
				bw.write(CI);
				bw.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				Starter.cfLogger.error("IOException",e);
			}
		}
		
	}
	
	private void WriteFooterAndCloseFile()
	{
		try {
			GZIPSDIFileGenerator.this.bw.write(getFooter());
			GZIPSDIFileGenerator.this.bw.flush();
			GZIPSDIFileGenerator.this.bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			Starter.cfLogger.error("IOException",e);
		}
		
		File thisTempFile = new File(this.Location, this.FileName+".temp");
		File thisFile = new File(this.Location, this.FileName);

		// Delete temp file if it exists
		if (thisFile.exists() == true) {
			thisFile.delete();
		}						

		FileUtilities.MoveFile(thisTempFile, thisFile);	
	}
	
	
	
	protected abstract String getHeader();

	protected abstract String getFooter();
	
	protected abstract String convertContentItem(final Map<String, SDICursorRow[]> hmCTypeRows);
	

	private class Worker implements Runnable {
		
		public Worker()
		{
			synchronized (GZIPSDIFileGenerator.this) {
				GZIPSDIFileGenerator.this.ActiveNum++;
			}
		}

		public void run() {

			try
			{
				while (true) {				

					HashMap<String,SDICursorRow[]> SCR = GZIPSDIFileGenerator.this.InQ.getObj();
					if (SCR != null)
					{
						try {
							GZIPSDIFileGenerator.this.WriteContentItem(convertContentItem(SCR));
						} catch (Exception e) {
							// TODO Auto-generated catch block
							Starter.cfLogger.error("Unknown Exception",e);
						}
					}
					else 
					{
						break;
					}					

				}
			}
			finally
			{

				synchronized (GZIPSDIFileGenerator.this) {
					GZIPSDIFileGenerator.this.ActiveNum--;

					if (GZIPSDIFileGenerator.this.ActiveNum==0)
					{
						try 
						{
							GZIPSDIFileGenerator.this.WriteFooterAndCloseFile();
						} 
						finally
						{
							Starter.cfLogger.info("SDI File: "+GZIPSDIFileGenerator.this.FileName+" has been generated");
							GZIPSDIFileGenerator.this.count.Decrease();
						}
					}

				}
			}

		}
	}
	
	protected String getXML(Document doc)
	{
		 try {
			TransformerFactory tf = TransformerFactory.newInstance();
			 Transformer trans = tf.newTransformer();
			 
			 java.io.StringWriter sw = new java.io.StringWriter();
			 StreamResult sr = new StreamResult(sw);
			 trans.transform(new DOMSource(doc),sr);
			 return sr.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		} 
	}
	
	protected String getDocumentAsXml(Document doc) {
		try {
			DOMSource domSource = new DOMSource(doc);

			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION,
					"yes");
			transformer.setOutputProperty(OutputKeys.METHOD, "xml");
			// transformer.setOutputProperty(OutputKeys.ENCODING,"ISO-8859-1");
			transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			// we want to pretty format the XML output
			// note : this is broken in jdk1.5 beta!
			transformer.setOutputProperty(
					"{http://xml.apache.org/xslt}indent-amount", "4");
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			//
			java.io.StringWriter sw = new java.io.StringWriter();

			StreamResult sr = new StreamResult(sw);
			transformer.transform(domSource, sr);
			return sw.toString();

		} catch (Exception e) {
			Starter.cfLogger.warn(
					"Failed to transfor XML doc to String : " + doc
							+ "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",e);
			return "";
			// throw new SystemException(e);
		}
	}
	
	protected Element CreatePermanentObject(Document doc,OracleObject oo, String outerTag)
	{
	    Long object_id = oo.getLong("object_id");
	    Long object_type_id = oo.getLong("object_type_id");
	    String object_type=oo.getString("object_type");

	    Element elOuterTag = doc.createElement(outerTag);
	    elOuterTag.appendChild(doc.createTextNode(String.valueOf((object_id))));

	    if (object_type_id !=null)
	    {
		elOuterTag.setAttribute("objectTypeId", String.valueOf((object_type_id)));
	    }

	    if (object_type !=null)
	    {
		elOuterTag.setAttribute("objectType", object_type);
	    }		

	    return elOuterTag;

	}
	
	protected Element CreateRelationObject(Document doc,Long commodity_flow_id, OracleObject oo, String outerTag)
	{
		
		Long object_id = oo.getLong("object_id");
		Long relation_object_type_id = oo.getLong("relation_object_type_id");
		String relation_object_type=oo.getString("relation_object_type");
		Long relationship_id=oo.getLong("relationship_id");
		Long relationship_type_id=oo.getLong("relationship_type_id");
		Long related_object_id=oo.getLong("related_object_id");
		Long related_object_type_id=oo.getLong("related_object_type_id");
		String related_object_type=oo.getString("related_object_type");
		String relation_role=oo.getString("relation_role");
		String relationship_type=oo.getString("relationship_type");
		Float relationship_confidence=oo.getFloat("relationship_confidence");
		String relation_object_na_code=oo.getString("relation_object_na_code");
		Long related_object_order=oo.getLong("related_object_order");
		Long relation_object_order=oo.getLong("relation_object_order");
		Timestamp effective_from=oo.getTimestamp("effective_from");
		Timestamp effective_to=oo.getTimestamp("effective_to");
		
		Element elOuterTag = doc.createElement(outerTag);
		elOuterTag.appendChild(doc.createTextNode(String.valueOf((object_id))));
		
		elOuterTag.setAttribute("relatedObjectId", String.valueOf(commodity_flow_id));
		
		if (relation_object_type_id !=null)
		{
			elOuterTag.setAttribute("relationObjectTypeId", String.valueOf((relation_object_type_id)));
		}
		
		
		if (relation_object_type !=null)
		{
			elOuterTag.setAttribute("relationObjectType", relation_object_type);
		}
		
		if (relationship_id != null)
		{
			elOuterTag.setAttribute("relationshipId", String.valueOf(relationship_id));
		}
		
		if (relationship_type_id != null)
		{
			elOuterTag.setAttribute("relationshipTypeId", String.valueOf(relationship_type_id));
		}
		
		if (related_object_id != null)
		{
			elOuterTag.setAttribute("relatedObjectId", String.valueOf(related_object_id));
		}
		
		if (related_object_type_id != null)
		{
			elOuterTag.setAttribute("relatedObjectTypeId", String.valueOf(related_object_type_id));
		}
		
		if (related_object_type !=null)
		{
			elOuterTag.setAttribute("relatedObjectType", related_object_type);
		}
		
		if (relation_role != null)
		{
			elOuterTag.setAttribute("relationRole", String.valueOf(relation_role));
		}
		
		if (relationship_type != null)
		{
			elOuterTag.setAttribute("relationshipType", String.valueOf(relationship_type));
		}
		
		if (relationship_confidence !=null)
		{
			elOuterTag.setAttribute("relationshipConfidence", String.valueOf(relationship_confidence));
		}
		
		if (relation_object_na_code !=null)
		{
			elOuterTag.setAttribute("relationObjectNACode", relation_object_na_code);
		}
		
		if (related_object_order !=null)
		{
			elOuterTag.setAttribute("relatedObjectOrder", String.valueOf(related_object_order));
		}
		
		if (relation_object_order !=null)
		{
			elOuterTag.setAttribute("relationObjectOrder", String.valueOf(relation_object_order));
		}
		
		DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
		
		if (effective_from != null)
		{
			
			String effectiveFrom = formatter.format(effective_from);
			elOuterTag.setAttribute("effectiveFrom", effectiveFrom);
		}
		
		if (effective_to != null)
		{
			String effectiveto = formatter.format(effective_to);
			elOuterTag.setAttribute("effectiveTo", effectiveto);				
		}
		
		return elOuterTag;
		
	}

}
