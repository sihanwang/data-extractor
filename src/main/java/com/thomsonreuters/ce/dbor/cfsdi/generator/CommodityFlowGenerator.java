package com.thomsonreuters.ce.dbor.cfsdi.generator;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ArrayList;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;








import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.thomsonreuters.ce.dbor.cfsdi.SDIConstants;
import com.thomsonreuters.ce.dbor.cfsdi.Starter;
import com.thomsonreuters.ce.dbor.cfsdi.cursor.CursorType;
import com.thomsonreuters.ce.dbor.cfsdi.cursor.SDICursorRow;
import com.thomsonreuters.ce.dbor.cfsdi.cursor.struct.OracleObject;
import com.thomsonreuters.ce.dbor.cfsdi.util.Counter;
import com.thomsonreuters.ce.queue.Pipe;

public class CommodityFlowGenerator extends GZIPSDIFileGenerator {

	//private static DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_S);
	
	public CommodityFlowGenerator(final String sdiFileLocation,
			final String physicalAssetName, final Date sdiStartTime,
			final Date sdiEndTime, final int Thread_Num,
			final Pipe<HashMap<String, SDICursorRow[]>> InPort,
			final Counter cursorCounter) {
		
		super(sdiFileLocation, physicalAssetName, sdiStartTime, sdiEndTime,
				Thread_Num, InPort, cursorCounter);

	}

	@Override
	protected String convertContentItem(
			final Map<String, SDICursorRow[]> hmCTypeRows) {

		String content = "";
		
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.newDocument();
			doc.setStrictErrorChecking(false);
			Element elContentItem = doc.createElement("env:ContentItem");
			doc.appendChild(elContentItem);
			Element elData = doc.createElement("env:Data");
			elData.setAttribute("type","CommodityFlow");
			elContentItem.appendChild(elData);
			Element elCF = doc.createElement("CommodityFlow");
			elData.appendChild(elCF);
			
			
			//Get data out of commodity_flow_common_info cursor
			SDICursorRow[] commodity_flow_common_info = hmCTypeRows.get(CursorType.COMMODITY_FLOW_COMMON_INFO);
			long commodity_flow_id=commodity_flow_common_info[0].getPerm_ID();
			
			OracleObject cne_flow_id = new OracleObject(OracleObject.PERMANENT_ID_IDENTIFIER_TYPE,commodity_flow_common_info[0].getObjects("CNE_FLOW_ID"));
			Date entity_create_date=commodity_flow_common_info[0].getTimestamp("ENTITY_CREATE_DATE");
			String admin_status=commodity_flow_common_info[0].getString("ADMIN_STATUS");
			Date admin_status_effective_from=commodity_flow_common_info[0].getTimestamp("ADMIN_STATUS_EFFECTIVE_FROM");
			String flow_status=commodity_flow_common_info[0].getString("FLOW_STATUS");
			OracleObject asset_id=new OracleObject(OracleObject.RELATION_OBJECT_ID_TYPE, commodity_flow_common_info[0].getObjects("ASSET_ID"));
			Float volume=commodity_flow_common_info[0].getFloat("VOLUME");
			String volume_measurement=commodity_flow_common_info[0].getString("VOLUME_MEASUREMENT");
			Float freight_value=commodity_flow_common_info[0].getFloat("FREIGHT_VALUE");
			String freight_measurement=commodity_flow_common_info[0].getString("FREIGHT_MEASUREMENT");
			OracleObject freight_currency_id=new OracleObject(OracleObject.RELATION_OBJECT_ID_TYPE, commodity_flow_common_info[0].getObjects("FREIGHT_CURRENCY_ID"));
			String freight_currency_rcs=commodity_flow_common_info[0].getString("FREIGHT_CURRENCY_RCS");
			String freight_currency=commodity_flow_common_info[0].getString("FREIGHT_CURRENCY");
			String freight_type=commodity_flow_common_info[0].getString("FREIGHT_TYPE");
			String comment_type=commodity_flow_common_info[0].getString("COMMENT_TYPE");
			String comments=commodity_flow_common_info[0].getString("COMMENTS");
			
			////////////////////////////////
			//Action
			String action = this.getAction(entity_create_date);
			elContentItem.setAttribute("action", action);
			
			///////////////////////////////
			// add <CommodityFlowID>
			///////////////////////////////
			if (cne_flow_id != null)
			{
			    if (cne_flow_id.getLong("object_id")!=null)
			    {
				Element elCFID=CreatePermanentObject(doc, cne_flow_id, "CommodityFlowID");			
				elCF.appendChild(elCFID);
			    }
			}
			
			///////////////////////////////
			// add <AdminStatus>
			///////////////////////////////

			Element elAS= doc.createElement("AdminStatus");
			
			if (admin_status_effective_from !=null)
			{
				DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
				String stradmin_status_effective_from = formatter.format(admin_status_effective_from);			
				elAS.setAttribute("effectiveFrom", stradmin_status_effective_from);
			}
			
			
			elAS.appendChild(doc.createTextNode(admin_status));
			elCF.appendChild(elAS);
			
			///////////////////////////////
			//Add <FlowStatus>
			///////////////////////////////
			if (flow_status!=null)
			{
				Element elFS = doc.createElement("FlowStatus");
				elFS.appendChild(doc.createTextNode(flow_status));
				elCF.appendChild(elFS);			
			}
			else
			{
				Starter.cfLogger.warn("Get empty flow_status for commodity flow id:"+commodity_flow_id);
			}
			
			
			///////////////////////////////
			// add <AssetID>
			///////////////////////////////
			
			if (asset_id != null)
			{
				if (asset_id.getLong("object_id")!=null)
				{
					Element elAID=CreateRelationObject(doc,commodity_flow_id,asset_id, "AssetID");			
					elCF.appendChild(elAID);
				}
			}
			
			
			////////////////////////////////
			//add CommodityFlowOrganisation
			///////////////////////////////
			
			SDICursorRow[] commodity_flow_organisation_info = hmCTypeRows.get(CursorType.COMMODITY_FLOW_ORGANISATION_INFO);
			if (commodity_flow_organisation_info != null)
			{
				Element[] elCFORs =CreateCommodityFlowOrganisation(doc,commodity_flow_id,commodity_flow_organisation_info);
				
				for (Element elCFOR : elCFORs)
				{
					elCF.appendChild(elCFOR);
				}
			}
			
			///////////////////////////////
			//add CommodityFlowCommodityType
			///////////////////////////////
			
			SDICursorRow[] commodity_flow_commodity_info = hmCTypeRows.get(CursorType.COMMODITY_FLOW_COMMODITY_INFO);
			if (commodity_flow_commodity_info != null)
			{
				Element[] elCFCIs =CreateCommodityFlowCommodityType(doc,commodity_flow_id,commodity_flow_commodity_info);
				
				for (Element elCFCI : elCFCIs)
				{
					elCF.appendChild(elCFCI);
				}
			}
			
			///////////////////////////////		
			//Add commodityFlowLoad,CommodityFlowDeparture,CommodityFlowDischarge,CommodityFlowArrival
			///////////////////////////////
			
			SDICursorRow[] commodity_flow_load_disch_info=hmCTypeRows.get(CursorType.COMMODITY_FLOW_LOAD_DISCH_INFO);
			SDICursorRow[] commodity_flow_arrive_departure_info=hmCTypeRows.get(CursorType.COMMODITY_FLOW_ARRIVE_DEPARTURE_INFO);
			
			ArrayList<Element> elLDDAs = CreateCommodityFlow_Load_Departure_Discharge_Arrival(doc,commodity_flow_id,commodity_flow_load_disch_info,commodity_flow_arrive_departure_info);
			for (Element elLDDA: elLDDAs )
			{
				elCF.appendChild(elLDDA);
			}
			
			///////////////////////////////
			//Add CommodityFlowVolume
			///////////////////////////////
			
			if (volume != null || volume_measurement!=null )
			{
				Element elCFV = doc.createElement("CommodityFlowVolume");
				if (volume != null)
				{
					Element elV = doc.createElement("Volume");
					elV.appendChild(doc.createTextNode(String.valueOf(volume)));
					elCFV.appendChild(elV);
				}
				
				if (volume_measurement!=null)
				{
					Element elVM = doc.createElement("VolumeMeasurement");
					elVM.setAttribute("languageId", "505062");
					elVM.appendChild(doc.createTextNode(volume_measurement));
					elCFV.appendChild(elVM);
				}				
				elCF.appendChild(elCFV);			
			}
			
			///////////////////////////////
			//Add CommodityFlowFreight
			///////////////////////////////
			if (freight_value != null ||
				freight_measurement != null ||
				freight_currency_id != null || 
				freight_currency_rcs != null|| 
				freight_currency != null ||
				freight_type != null)
			{
				Element elCFF=doc.createElement("CommodityFlowFreight");
				
				if (freight_value != null)
				{
					Element elFV=doc.createElement("FreightValue");
					elFV.appendChild(doc.createTextNode(String.valueOf(freight_value)));
					elCFF.appendChild(elFV);
				}
				
				if (freight_measurement != null)
				{
					Element elFM=doc.createElement("FreightMeasurement");
					elFM.setAttribute("languageId", "505062");
					elFM.appendChild(doc.createTextNode(freight_measurement));
				}
				
				if (freight_currency_id != null)
				{
					Element elFCID=CreateRelationObject(doc,commodity_flow_id,freight_currency_id, "FreightCurrencyID");
					elCFF.appendChild(elFCID);
				}
				
				if (freight_currency_rcs != null)
				{
					Element elFCR=doc.createElement("FreightCurrencyRCS");
					elFCR.appendChild(doc.createTextNode(freight_currency_rcs));
					elCFF.appendChild(elFCR);
				}
				
				if (freight_currency != null)
				{
					Element elFC=doc.createElement("FreightCurrency");
					elFC.appendChild(doc.createTextNode(freight_currency));
					elCFF.appendChild(elFC);
				}
				
				if (freight_type != null)
				{
					Element elFT=doc.createElement("FreightType");
					elFT.appendChild(doc.createTextNode(freight_type));
					elCFF.appendChild(elFT);
				}
				
				elCF.appendChild(elCFF);
			}
			///////////////////////////////
			//Add CommodityFlowComment
			///////////////////////////////
			
			if (comments != null || comment_type !=null)
			{
				Element elCFC=doc.createElement("CommodityFlowComment");
				
				if (comments != null)
				{
					Element elC=doc.createElement("Comment");
					elC.appendChild(doc.createTextNode(comments));
					elCFC.appendChild(elC);
				}
				
				if (comment_type !=null)
				{
					Element elCT=doc.createElement("CommentType");
					elCT.appendChild(doc.createTextNode(comment_type));
					elCFC.appendChild(elCT);
				}
				
				elCF.appendChild(elCFC);
			}
						
			//add FixtureID , TenderID, PIERSID
			SDICursorRow[] commodity_flow_linkage_info = hmCTypeRows.get(CursorType.COMMODITY_FLOW_LINKAGE_INFO);
			if (commodity_flow_linkage_info != null)
			{
				ArrayList<Element> Fixture_Tender_Piers = CreateFixture_Tender_Piers(doc,commodity_flow_linkage_info);

				for (Element FTP :  Fixture_Tender_Piers)
				{
					elCF.appendChild(FTP);
				}
			}
			
			///////////////////////////////
			//add RelatedCommodityFlow
			///////////////////////////////
			SDICursorRow[] commodity_flow_flow_rship_info= hmCTypeRows.get(CursorType.COMMODITY_FLOW_FLOW_RSHIP_INFO);
			if (commodity_flow_flow_rship_info !=null)
			{			
				for (SDICursorRow SCR : commodity_flow_flow_rship_info)
				{
					OracleObject RelatedCommodityFlowID=new OracleObject(OracleObject.RELATION_OBJECT_ID_TYPE, SCR.getObjects("RELATED_COMMODITY_FLOW_ID"));
					String RelatedCommodityFlowRelationshipType=SCR.getString("RELATIONSHIP_TYPE");

					Element elRCF=doc.createElement("RelatedCommodityFlow");				
					elRCF.appendChild(CreateRelationObject(doc,commodity_flow_id,RelatedCommodityFlowID, "RelatedCommodityFlowID"));
					
					if (RelatedCommodityFlowRelationshipType != null)
					{
						Element elRCFT=doc.createElement("RelatedCommodityFlowRelationshipType");
						elRCFT.appendChild(doc.createTextNode(RelatedCommodityFlowRelationshipType));
						elRCF.appendChild(elRCFT);
					}
					
					elCF.appendChild(elRCF);
				}
			}
			
			content=getDocumentAsXml(doc);
			content = content.replaceAll("type=\"CommodityFlow\">",
			"xsi:type=\"CommodityFlow\">");
			
		} catch (Exception e) {
			Starter.cfLogger.error("Exception is captured while generating ContentItem: ", e);
		}
		
		return content;
	}


	@Override
	protected String getFooter() {
		return "		</env:Body>\r\n" + "</env:ContentEnvelope>";
	}

	@Override
	protected String getHeader() {
		String envolope = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n"
				+ "<env:ContentEnvelope xmlns:env=\"http://data.schemas.tfn.thomson.com/Envelope/2008-05-01/\" xmlns=\"http://CommodityFlows.schemas.financial.thomsonreuters.com/2012-05-30/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://CommodityFlows.schemas.financial.thomsonreuters.com/2012-05-30/ CommodityFlow.xsd\"  pubStyle=\"@PubType@\" majVers=\"3\" minVers=\"0.0\">\r\n";

		String header = envolope
				+ "	<env:Header>\r\n"
				+ "		<env:Info>\r\n"
				+ "			<env:Id>urn:uuid:@GUID@</env:Id>\r\n"
				+ "			<env:TimeStamp>@TIMESTAMP@</env:TimeStamp>\r\n"
				+ "		</env:Info>\r\n"
				+ "</env:Header>\r\n"
				+ "<env:Body contentSet=\"CommodityFlows\" majVers=\"1\" minVers=\"0.0\">\r\n";

		String uuid = UUID.randomUUID().toString();
		header = header.replace("@GUID@", uuid);
		Date XMLTimestamp = new Date();// yyyy-MM-dd HH:mm:ss.SSS
		DateFormat dataFormat = new SimpleDateFormat(
				SDIConstants.DATE_FORMAT_SSSZ, Locale.getDefault());

		// DateFormat df = DateFormat.getDateTimeInstance(DateFormat.MEDIUM,
		// DateFormat.MEDIUM);
		String strDate = dataFormat.format(XMLTimestamp);
		header = header.replace("@TIMESTAMP@", strDate);
		if (this.IsFullLoad()) {
			header = header.replace("@PubType@", "FullRebuild");
		} else {
			header = header.replace("@PubType@", "Incremental");
		}

		return header;
	}
	
	private Element[] CreateCommodityFlowOrganisation(Document doc,Long commodity_flow_id,SDICursorRow[] cursorCFO)
	{
		Element[] CFOs= new Element[cursorCFO.length];
		
		for (int i=0; i<cursorCFO.length;i++)
		{
			SDICursorRow singleRow=cursorCFO[i];
			
			Element CFO=doc.createElement("CommodityFlowOrganisation");
			
			OracleObject organisation_id=new OracleObject(OracleObject.RELATION_OBJECT_ID_TYPE, singleRow.getObjects("ORGANISATION_ID"));
			Long organisation_internal_id=singleRow.getLong("ORGANISATION_INTERNAL_ID");
			String organisation_name=singleRow.getString("ORGANISATION_NAME");
			String organisation_role=singleRow.getString("ORGANISATION_ROLE");
			Integer organisation_rank=singleRow.getInteger("ORGANISATION_RANK");
			
			if (organisation_id !=null)
			{
				if (organisation_id.getLong("object_id")!=null)
				{
					Element elCFOI=CreateRelationObject(doc,commodity_flow_id,organisation_id, "CommodityFlowOrganisationID");			
					CFO.appendChild(elCFOI);
				}
			}
			
			if (organisation_internal_id!=null)
			{
				Element elOII=doc.createElement("CommodityFlowOrganisationIntID");
				elOII.appendChild(doc.createTextNode(String.valueOf(organisation_internal_id)));
				CFO.appendChild(elOII);
			}
			
			if (organisation_name != null)
			{	
				Element elON=doc.createElement("CommodityFlowOrganisationName");
				elON.setAttribute("languageId", "505062");
				elON.appendChild(doc.createTextNode(organisation_name));
				CFO.appendChild(elON);
			}
			
			if (organisation_role != null)
			{	
				Element elOR=doc.createElement("CommodityFlowOrganisationRole");
				elOR.setAttribute("languageId", "505062");
				elOR.appendChild(doc.createTextNode(organisation_role));
				CFO.appendChild(elOR);
			}
			
			if (organisation_rank != null)
			{
				Element elCOR = doc.createElement("CommodityFlowOrganisationRank");
				elCOR.appendChild(doc.createTextNode(String.valueOf(organisation_rank)));
				CFO.appendChild(elCOR);
			}
			
			CFOs[i]=CFO;
		}
		
		return CFOs;
	}
	
	private Element[] CreateCommodityFlowCommodityType(Document doc,Long commodity_flow_id,SDICursorRow[] cursorCFCT)
	{
		Element[] CFCTs= new Element[cursorCFCT.length];
		
		for (int i=0 ; i<cursorCFCT.length; i++)
		{
			
			SDICursorRow singleRow=cursorCFCT[i];
			
			Element CFCT=doc.createElement("CommodityFlowCommodityType");
			
			OracleObject comm_id=new OracleObject(OracleObject.RELATION_OBJECT_ID_TYPE, singleRow.getObjects("COMM_ID"));
			String comm_rcs=singleRow.getString("COMM_RCS");
			String comm=singleRow.getString("COMM");
			Integer comm_sub_type_id=singleRow.getInteger("COMM_SUB_TYPE_ID");
			String comm_sub_type=singleRow.getString("COMM_SUB_TYPE");
			String comm_sub_type_forecast_actual=singleRow.getString("COMM_SUB_TYPE_FORECAST_ACTUAL");
			OracleObject countryoforiginid =new OracleObject(OracleObject.RELATION_OBJECT_ID_TYPE, singleRow.getObjects("COUNTR_OF_ORIGIN_ID"));
			String countryoforiginrcs=singleRow.getString("COUNTR_OF_ORIGIN_RCS");
			String countryoforigin=singleRow.getString("COUNTR_OF_ORIGIN");
			
			if (comm_id != null)
			{
				if (comm_id.getLong("object_id")!=null)
				{
					Element elCI=CreateRelationObject(doc,commodity_flow_id,comm_id, "CommID");
					CFCT.appendChild(elCI);
				}
			}
			
			if (comm_rcs != null)
			{	
				Element elCR=doc.createElement("CommRCS");
				elCR.appendChild(doc.createTextNode(comm_rcs));
				CFCT.appendChild(elCR);
			}
			if (comm != null)
			{	
				Element elC=doc.createElement("Comm");
				elC.setAttribute("languageId", "505062");
				elC.appendChild(doc.createTextNode(comm));
				CFCT.appendChild(elC);
			}
			if (comm_sub_type_id != null)
			{	
				Element elCSTI=doc.createElement("CommoditySubTypeID");
				elCSTI.appendChild(doc.createTextNode(String.valueOf(comm_sub_type_id)));
				CFCT.appendChild(elCSTI);
			}
			if (comm_sub_type != null)
			{	
				Element elCST=doc.createElement("CommoditySubType");
				elCST.setAttribute("languageId", "505062");
				elCST.appendChild(doc.createTextNode(comm_sub_type));
				CFCT.appendChild(elCST);
			}
			if (comm_sub_type_forecast_actual != null)
			{	
				Element elCSTFA=doc.createElement("CommoditySubTypeForecastActual");
				elCSTFA.setAttribute("languageId", "505062");
				elCSTFA.appendChild(doc.createTextNode(comm_sub_type_forecast_actual));
				CFCT.appendChild(elCSTFA);
			}
			if (countryoforiginid != null)
			{
			    if (countryoforiginid.getLong("object_id")!=null)
			    {
				Element elCOOI=CreateRelationObject(doc,commodity_flow_id,countryoforiginid, "CountryOfOriginID");
				CFCT.appendChild(elCOOI);
			    }
			}
			if (countryoforiginrcs != null)
			{
				Element elCOOR=doc.createElement("CountryOfOriginRCS");
				elCOOR.appendChild(doc.createTextNode(countryoforiginrcs));
				CFCT.appendChild(elCOOR);
			}
			if (countryoforigin != null)
			{
				Element elCOO=doc.createElement("CountryOfOrigin");
				elCOO.setAttribute("languageId", "505062");
				elCOO.appendChild(doc.createTextNode(countryoforigin));
				CFCT.appendChild(elCOO);
			}
			
			
			
			CFCTs[i]=CFCT;
			
		}
		
		return CFCTs;
		
	}
	
	private ArrayList<Element> CreateCommodityFlow_Load_Departure_Discharge_Arrival(Document doc,Long commodity_flow_id,SDICursorRow[] cursorCFLDI, SDICursorRow[] cursorCFADI)
	{
		//Element for CommodityFlowLoad
		ArrayList<Element> elCFLs = new ArrayList<Element>();
		
		//Element for CommodityFlowDeparture
		ArrayList<Element> elCFDepartures= new ArrayList<Element>();

		//Element for CommodityFlowDischarge
		ArrayList<Element> elCFDischarges= new ArrayList<Element>();
		
		//Element for CommodityFlowArrival
		ArrayList<Element> elCFAs= new ArrayList<Element>();
		
		if (cursorCFLDI != null)
		{

			for (int i=0; i<cursorCFLDI.length;i++ )
			{
				SDICursorRow singleRow=cursorCFLDI[i];
				String action_type=singleRow.getString("ACTION_TYPE");
				OracleObject ACTION_GEOGRAPHY_ID=new OracleObject(OracleObject.RELATION_OBJECT_ID_TYPE, singleRow.getObjects("ACTION_GEOGRAPHY_ID"));
				String ACTION_GEOGRAPHY_RCS=singleRow.getString("ACTION_GEOGRAPHY_RCS");
				String ACTION_GEOGRAPHY=singleRow.getString("ACTION_GEOGRAPHY");
				OracleObject ACTION_PORT_ID=new OracleObject(OracleObject.RELATION_OBJECT_ID_TYPE, singleRow.getObjects("ACTION_PORT_ID"));
				
				String ACTION_FCST_ACTL=singleRow.getString("ACTION_FCST_ACTL");
				Date ACTION_DATE_FROM=singleRow.getTimestamp("ACTION_DATE_FROM");
				Date ACTION_DATE_TO=singleRow.getTimestamp("ACTION_DATE_TO");
				String ACTION_DATE_FCST_ACTL=singleRow.getString("ACTION_DATE_FCST_ACTL");
				Date ACTION_DATE=singleRow.getTimestamp("ACTION_DATE");


				//TO BE FINISHED WITH MORE COLUMNS

				if (action_type.equals("LOAD"))
				{
					Element elCFL=doc.createElement("CommodityFlowLoad");
					if (ACTION_GEOGRAPHY_ID != null)
					{
						if (ACTION_GEOGRAPHY_ID.getLong("object_id")!=null)
						{
							Element elLGID=CreateRelationObject(doc,commodity_flow_id,ACTION_GEOGRAPHY_ID, "LoadGeographyID");
							elCFL.appendChild(elLGID);			
						}
					}

					if (ACTION_GEOGRAPHY_RCS != null)
					{
						Element elLGR=doc.createElement("LoadGeographyRCS");
						elLGR.appendChild(doc.createTextNode(ACTION_GEOGRAPHY_RCS));
						elCFL.appendChild(elLGR);
					}

					if (ACTION_GEOGRAPHY != null)
					{
						Element elG=doc.createElement("LoadGeography");
						elG.setAttribute("languageId", "505062");
						elG.appendChild(doc.createTextNode(ACTION_GEOGRAPHY));
						elCFL.appendChild(elG);					
					}

					if (ACTION_PORT_ID != null)
					{
						if (ACTION_PORT_ID.getLong("object_id")!=null)
						{
							Element elLPI=CreateRelationObject(doc,commodity_flow_id,ACTION_PORT_ID, "LoadPortID");			
							elCFL.appendChild(elLPI);
						}
					}
					
					
					if (ACTION_FCST_ACTL != null)
					{
						Element elLGFA=doc.createElement("LoadGeographyForecastActual");
						elLGFA.appendChild(doc.createTextNode(ACTION_FCST_ACTL));
						elCFL.appendChild(elLGFA);					
					}

					if (ACTION_DATE_FROM!= null)
					{
						Element elLDF=doc.createElement("LoadDateFrom");
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
						String LoadDateFrom = formatter.format(ACTION_DATE_FROM);	
						elLDF.appendChild(doc.createTextNode(LoadDateFrom));
						elCFL.appendChild(elLDF);	
					}

					if (ACTION_DATE_TO!= null)
					{
						Element elADT=doc.createElement("LoadDateTo");
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
						String LoadDateTo = formatter.format(ACTION_DATE_TO);	
						elADT.appendChild(doc.createTextNode(LoadDateTo));
						elCFL.appendChild(elADT);	
					}

					if (ACTION_DATE_FCST_ACTL!= null)
					{
						Element elADFA=doc.createElement("LoadDateForecastActual");
						elADFA.appendChild(doc.createTextNode(ACTION_DATE_FCST_ACTL));
						elCFL.appendChild(elADFA);	
					}
					
					if (ACTION_DATE!= null)
					{
						Element eLAD=doc.createElement("LoadDate");
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
						String LoadDate = formatter.format(ACTION_DATE);	
						eLAD.appendChild(doc.createTextNode(LoadDate));
						elCFL.appendChild(eLAD);	
					}

					elCFLs.add(elCFL);

				}
				else if (action_type.equals("DISCH"))
				{
					Element elCFD=doc.createElement("CommodityFlowDischarge");

					if (ACTION_GEOGRAPHY_ID != null)
					{
						if (ACTION_GEOGRAPHY_ID.getLong("object_id")!=null)
						{
							Element elDGID=CreateRelationObject(doc,commodity_flow_id,ACTION_GEOGRAPHY_ID, "DischargeGeographyID");
							elCFD.appendChild(elDGID);			
						}
					}

					if (ACTION_GEOGRAPHY_RCS != null)
					{
						Element elDGR=doc.createElement("DischargeGeographyRCS");
						elDGR.appendChild(doc.createTextNode(ACTION_GEOGRAPHY_RCS));
						elCFD.appendChild(elDGR);
					}

					if (ACTION_GEOGRAPHY != null)
					{
						Element elDG=doc.createElement("DischargeGeography");
						elDG.setAttribute("languageId", "505062");
						elDG.appendChild(doc.createTextNode(ACTION_GEOGRAPHY));
						elCFD.appendChild(elDG);					
					}


					if (ACTION_PORT_ID != null)
					{
						if (ACTION_PORT_ID.getLong("object_id")!=null)
						{
							Element elDPI=CreateRelationObject(doc,commodity_flow_id,ACTION_PORT_ID, "DischargePortID");			
							elCFD.appendChild(elDPI);
						}
					}

					if (ACTION_FCST_ACTL != null)
					{
						Element elDGFA=doc.createElement("DischargeGeographyForecastActual");
						elDGFA.appendChild(doc.createTextNode(ACTION_FCST_ACTL));
						elCFD.appendChild(elDGFA);					
					}
					if (ACTION_DATE!= null)
					{
						Element eLAD=doc.createElement("DischargeDate");
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
						String DischargeDate = formatter.format(ACTION_DATE);	
						eLAD.appendChild(doc.createTextNode(DischargeDate));
						elCFD.appendChild(eLAD);	
					}

					elCFDischarges.add(elCFD);

				}
			}
		}
		
		if (cursorCFADI!=null)
		{

			for (int i=0 ; i<cursorCFADI.length;i++)
			{
				SDICursorRow singleRow=cursorCFADI[i];
				Date ACTION_DATE_FROM=singleRow.getTimestamp("ACTION_DATE_FROM");
				Date ACTION_DATE_TO=singleRow.getTimestamp("ACTION_DATE_TO");
				String ACTION_TYPE=singleRow.getString("ACTION_TYPE");
				Date ACTION_DATE=singleRow.getTimestamp("ACTION_DATE");
				String ACTION_DATE_FCST_ACTUAL=singleRow.getString("ACTION_DATE_FCST_ACTUAL");
				String ACTION_EST_TYPE=singleRow.getString("ACTION_EST_TYPE");

				if (ACTION_TYPE.equals("DEPARTURE"))
				{
					Element elCFD=doc.createElement("CommodityFlowDeparture");
					
					if (ACTION_DATE_FROM != null)
					{
						Element elADF=doc.createElement("DepartureDateFrom");
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
						String DepartureDateFrom = formatter.format(ACTION_DATE_FROM);
						elADF.appendChild(doc.createTextNode(DepartureDateFrom));
						elCFD.appendChild(elADF);
					}
					
					if (ACTION_DATE_TO != null)
					{
						Element elADT=doc.createElement("DepartureDateTo");
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
						String DepartureDateTo = formatter.format(ACTION_DATE_TO);
						elADT.appendChild(doc.createTextNode(DepartureDateTo));
						elCFD.appendChild(elADT);
					}
					
					if (ACTION_DATE != null)
					{
						Element elDD=doc.createElement("DepartureDate");
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
						String DepartureDate = formatter.format(ACTION_DATE);
						elDD.appendChild(doc.createTextNode(DepartureDate));
						elCFD.appendChild(elDD);
					}

					if (ACTION_DATE_FCST_ACTUAL != null)
					{
						Element elDDFA=doc.createElement("DepartureDateForecastActual");
						elDDFA.setAttribute("languageId", "505062");
						elDDFA.appendChild(doc.createTextNode(ACTION_DATE_FCST_ACTUAL));
						elCFD.appendChild(elDDFA);
					}

					if (ACTION_EST_TYPE != null)
					{
						Element elDDET=doc.createElement("DepartureDateEstimateType");
						elDDET.setAttribute("languageId", "505062");
						elDDET.appendChild(doc.createTextNode(ACTION_EST_TYPE));
						elCFD.appendChild(elDDET);
					}

					elCFDepartures.add(elCFD);

				}
				else if (ACTION_TYPE.equals("ARRIVAL"))
				{
					Element elCFA=doc.createElement("CommodityFlowArrival");

					if (ACTION_DATE_FROM != null)
					{
						Element elADF=doc.createElement("ArrivalDateFrom");
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
						String DepartureDateFrom = formatter.format(ACTION_DATE_FROM);
						elADF.appendChild(doc.createTextNode(DepartureDateFrom));
						elCFA.appendChild(elADF);
					}
					
					if (ACTION_DATE_TO != null)
					{
						Element elADT=doc.createElement("ArrivalDateTo");
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
						String DepartureDateTo = formatter.format(ACTION_DATE_TO);
						elADT.appendChild(doc.createTextNode(DepartureDateTo));
						elCFA.appendChild(elADT);
					}
					
					if (ACTION_DATE != null)
					{
						Element elAD=doc.createElement("ArrivalDate");
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
						String ArrivalDate = formatter.format(ACTION_DATE);
						elAD.appendChild(doc.createTextNode(ArrivalDate));
						elCFA.appendChild(elAD);
					}

					if (ACTION_DATE_FCST_ACTUAL != null)
					{
						Element elADFA=doc.createElement("ArrivalDateForecastActual");
						elADFA.setAttribute("languageId", "505062");
						elADFA.appendChild(doc.createTextNode(ACTION_DATE_FCST_ACTUAL));
						elCFA.appendChild(elADFA);
					}

					if (ACTION_EST_TYPE != null)
					{
						Element elADET=doc.createElement("ArrivalDateEstimateType");
						elADET.setAttribute("languageId", "505062");
						elADET.appendChild(doc.createTextNode(ACTION_EST_TYPE));
						elCFA.appendChild(elADET);
					}

					elCFAs.add(elCFA);

				}

			}
		}
		
		ArrayList<Element> Load_Departure_Discharge_Arrival = new ArrayList<Element>();
		Load_Departure_Discharge_Arrival.addAll(elCFLs);
		Load_Departure_Discharge_Arrival.addAll(elCFDepartures);
		Load_Departure_Discharge_Arrival.addAll(elCFDischarges);
		Load_Departure_Discharge_Arrival.addAll(elCFAs);
		
		return Load_Departure_Discharge_Arrival;
		
	}
	
	private ArrayList<Element> CreateFixture_Tender_Piers(Document doc, SDICursorRow[] commodity_flow_linkage_info)
	{
		ArrayList<Element> elFixtures = new ArrayList<Element>();
		ArrayList<Element> elTenders = new ArrayList<Element>();
		ArrayList<Element> elPierss = new ArrayList<Element>();
		ArrayList<Element> elSgss = new ArrayList<Element>();

		for (SDICursorRow cfli : commodity_flow_linkage_info) {
			String element_name = cfli.getString("ELEMENT_NAME");
			Long element_id = cfli.getLong("ELEMENT_ID");

			if (element_name.equals("FixtureID")) {
				Element elFI = doc.createElement("FixtureID");
				elFI.appendChild(doc.createTextNode(String.valueOf(element_id)));
				elFixtures.add(elFI);

			} else if (element_name.equals("TenderID")) {
				Element elTI = doc.createElement("TenderID");
				elTI.appendChild(doc.createTextNode(String.valueOf(element_id)));
				elTenders.add(elTI);

			} else if (element_name.equals("PIERSID")) {
				Element elPI = doc.createElement("PIERSID");
				elPI.appendChild(doc.createTextNode(String.valueOf(element_id)));
				elPierss.add(elPI);
			} else if (element_name.equals("SGSID")) {
				Element elSI = doc.createElement("SGSID");
				elSI.appendChild(doc.createTextNode(String.valueOf(element_id)));
				elSgss.add(elSI);
			}
		}

		ArrayList<Element> Fixture_Tender_Piers = new ArrayList<Element>();
		Fixture_Tender_Piers.addAll(elFixtures);
		Fixture_Tender_Piers.addAll(elTenders);
		Fixture_Tender_Piers.addAll(elPierss);
		Fixture_Tender_Piers.addAll(elSgss);

		return Fixture_Tender_Piers;}

	

}
