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

public class TradeGenerator extends GZIPSDIFileGenerator {

	public TradeGenerator(final String sdiFileLocation,
			final String physicalAssetName, final Date sdiStartTime,
			final Date sdiEndTime, final int Thread_Num,
			final Pipe<HashMap<String, SDICursorRow[]>> InPort,
			final Counter cursorCounter) {
		
		super(sdiFileLocation, physicalAssetName, sdiStartTime, sdiEndTime,
				Thread_Num, InPort, cursorCounter);

	}

	
	@Override
	protected String convertContentItem(Map<String, SDICursorRow[]> hmCTypeRows) {
		// TODO Auto-generated method stub
		String content = "";

		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.newDocument();
			doc.setStrictErrorChecking(false);
			Element elContentItem = doc.createElement("env:ContentItem");
			doc.appendChild(elContentItem);
			Element elData = doc.createElement("env:Data");
			elData.setAttribute("type","Trade");
			elContentItem.appendChild(elData);
			Element elT = doc.createElement("Trade");
			elData.appendChild(elT);

			SDICursorRow[] commodity_flow_common_info = hmCTypeRows.get(CursorType.COMMODITY_FLOW_COMMON_INFO);
			long commodity_flow_id=commodity_flow_common_info[0].getPerm_ID();

			OracleObject cne_flow_id = new OracleObject(OracleObject.PERMANENT_ID_IDENTIFIER_TYPE,commodity_flow_common_info[0].getObjects("CNE_FLOW_ID"));
			Date entity_create_date=commodity_flow_common_info[0].getTimestamp("ENTITY_CREATE_DATE");
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
					elT.appendChild(elCFID);
				}
			}		


			SDICursorRow[] commodity_trade_common_info=hmCTypeRows.get(CursorType.COMMODITY_TRADE_COMMON_INFO);

			if (commodity_trade_common_info != null)
			{

				for (SDICursorRow SCR : commodity_trade_common_info)
				{
					Long trade_id=SCR.getLong("TRADE_ID");
					Date effective_from=SCR.getTimestamp("TRADE_EFFECTIVE_FROM");
					Date effective_to=SCR.getTimestamp("TRADE_EFFECTIVE_TO");					
					Float trade_volume=SCR.getFloat("TRADE_VOLUME");
					Date trade_date=SCR.getTimestamp("TRADE_DATE");
					Date validity_date=SCR.getTimestamp("VALIDITY_DATE");
					Date closing_date=SCR.getTimestamp("CLOSING_DATE");
					Long trade_vol_measurement_id=SCR.getLong("TRADE_VOL_MEASUREMENT_ID");
					String trade_vol_measurement=SCR.getString("TRADE_VOL_MEASUREMENT");
					Float trade_price_low=SCR.getFloat("TRADE_PRICE_LOW");
					Float trade_price_high=SCR.getFloat("TRADE_PRICE_HIGH");
					OracleObject trade_price_currency_id=new OracleObject(OracleObject.RELATION_OBJECT_ID_TYPE, SCR.getObjects("TRADE_PRICE_CURRENCY_ID"));
					String trade_price_currency_rcs=SCR.getString("TRADE_PRICE_CURRENCY_RCS");
					String trade_price_currency=SCR.getString("TRADE_PRICE_CURRENCY");
					Long trade_price_measurement_id=SCR.getLong("TRADE_PRICE_MEASUREMENT_ID");
					String trade_price_measurement=SCR.getString("TRADE_PRICE_MEASUREMENT");
					String trade_discount_premium=SCR.getString("TRADE_DISCOUNT_PREMIUM");
					String trade_type=SCR.getString("TRADE_TYPE");
					String trade_price_basis=SCR.getString("TRADE_PRICE_BASIS");
					String trade_terms=SCR.getString("TRADE_TERMS");
					String trade_benchmark=SCR.getString("TRADE_BENCHMARK");
					String trade_comment=SCR.getString("TRADE_COMMENT");


					///////////////////////////////
					// add <CommodityFlowTrade>
					///////////////////////////////
					Element elCFT = doc.createElement("CommodityFlowTrade");
					elT.appendChild(elCFT);

					//add TradeID
					if (trade_id != null)
					{
						Element elTID = doc.createElement("TradeID");						
						elTID.appendChild(doc.createTextNode(String.valueOf(trade_id)));

						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
						if (effective_from!=null)
						{
							String streffective_from = formatter.format(effective_from);
							elTID.setAttribute("effectiveFrom", streffective_from);
						}
						if (effective_to!=null)
						{
							String streffective_to = formatter.format(effective_to);
							elTID.setAttribute("effectiveTo", streffective_to);
						}

						elCFT.appendChild(elTID);
					}

					//add TradeVolume
					if (trade_volume != null)
					{
						Element elTV = doc.createElement("TradeVolume");
						elTV.appendChild(doc.createTextNode(String.valueOf(trade_volume)));
						elCFT.appendChild(elTV);
					}

					//add TradeVolMeasurementID
					if (trade_vol_measurement_id != null)
					{
						Element elTVMI = doc.createElement("TradeVolMeasurementID");
						elTVMI.appendChild(doc.createTextNode(String.valueOf(trade_vol_measurement_id)));
						elCFT.appendChild(elTVMI);
					}

					//add TradeVolMeasurement
					if (trade_vol_measurement != null)
					{
						Element elTVM = doc.createElement("TradeVolMeasurement");
						elTVM.setAttribute("languageId", "505062");
						elTVM.appendChild(doc.createTextNode(trade_vol_measurement));
						elCFT.appendChild(elTVM);
					}

					//add TradePriceLow
					if (trade_price_low != null)
					{
						Element elTPL = doc.createElement("TradePriceLow");
						elTPL.appendChild(doc.createTextNode(String.valueOf(trade_price_low)));
						elCFT.appendChild(elTPL);
					}

					//add TradePriceHigh
					if (trade_price_high != null)
					{
						Element elTPH = doc.createElement("TradePriceHigh");
						elTPH.appendChild(doc.createTextNode(String.valueOf(trade_price_high)));
						elCFT.appendChild(elTPH);
					}

					// add TradePriceCurrencyID
					if (trade_price_currency_id != null)
					{
						if (trade_price_currency_id.getLong("object_id")!=null)
						{
							Element elTPCI=CreateRelationObject(doc,commodity_flow_id,trade_price_currency_id, "TradePriceCurrencyID");			
							elCFT.appendChild(elTPCI);
						}
					}

					//add TradePriceCurrencyRCS
					if (trade_price_currency_rcs != null)
					{
						Element elTPCR = doc.createElement("TradePriceCurrencyRCS");
						elTPCR.appendChild(doc.createTextNode(trade_price_currency_rcs));
						elCFT.appendChild(elTPCR);
					}

					//add TradePriceCurrency
					if (trade_price_currency != null)
					{
						Element elTPC = doc.createElement("TradePriceCurrency");
						elTPC.setAttribute("languageId", "505062");
						elTPC.appendChild(doc.createTextNode(trade_price_currency));
						elCFT.appendChild(elTPC);
					}

					//TradePriceMeasurementID
					if (trade_price_measurement_id != null)
					{
						Element elTPMI = doc.createElement("TradePriceMeasurementID");
						elTPMI.appendChild(doc.createTextNode(String.valueOf(trade_price_measurement_id)));
						elCFT.appendChild(elTPMI);
					}

					//add TradePriceMeasurement
					if (trade_price_measurement != null)
					{
						Element elTPM = doc.createElement("TradePriceMeasurement");
						elTPM.appendChild(doc.createTextNode(trade_price_measurement));
						elCFT.appendChild(elTPM);
					}

					//add TradeDiscountPremium
					if (trade_discount_premium != null)
					{
						Element elTDM = doc.createElement("TradeDiscountPremium");
						elTDM.appendChild(doc.createTextNode(trade_discount_premium));
						elCFT.appendChild(elTDM);
					}

					//add TradeType
					if (trade_type != null)
					{
						Element elTT = doc.createElement("TradeType");
						elTT.appendChild(doc.createTextNode(trade_type));
						elCFT.appendChild(elTT);
					}

					//add TradePriceBasis
					if (trade_price_basis != null)
					{
						Element elTPB = doc.createElement("TradePriceBasis");
						elTPB.appendChild(doc.createTextNode(trade_price_basis));
						elCFT.appendChild(elTPB);
					}

					//add TradeTerms
					if (trade_terms != null)
					{
						Element elTT = doc.createElement("TradeTerms");
						elTT.appendChild(doc.createTextNode(trade_terms));
						elCFT.appendChild(elTT);
					}

					//add TradeBenchmark
					if (trade_benchmark != null)
					{
						Element elTBM = doc.createElement("TradeBenchmark");
						elTBM.appendChild(doc.createTextNode(trade_benchmark));
						elCFT.appendChild(elTBM);
					}

					//Add TradeOrganisation

					SDICursorRow[] commodity_trade_organisation_info = hmCTypeRows.get(CursorType.COMMODITY_TRADE_ORGANISATION_INFO);
					if (commodity_trade_organisation_info != null)
					{
						ArrayList<Element> elCTOIs=CreateTradeOrganisation(doc,commodity_flow_id,trade_id,commodity_trade_organisation_info);

						for (Element elCTOI : elCTOIs)
						{
							elCFT.appendChild(elCTOI);
						}
					}

					if (closing_date != null)
					{
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
						String strclosing_date = formatter.format(closing_date);			
						Element elCD = doc.createElement("TradeClosingDate");
						elCD.appendChild(doc.createTextNode(strclosing_date));
						elCFT.appendChild(elCD);
					}

					if (validity_date != null)
					{
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
						String strvalidity_date = formatter.format(validity_date);			
						Element elVD = doc.createElement("TradeValidityDate");
						elVD.appendChild(doc.createTextNode(strvalidity_date));
						elCFT.appendChild(elVD);
					}		

					if (trade_date != null)
					{
						DateFormat formatter = new SimpleDateFormat(SDIConstants.DATE_FORMAT_SSSZ);
						String strtrade_date = formatter.format(trade_date);			
						Element elTD = doc.createElement("TradeDate");
						elTD.appendChild(doc.createTextNode(strtrade_date));
						elCFT.appendChild(elTD);
					}						

					//add TradeComment
					if (trade_comment != null)
					{
						Element elTC = doc.createElement("TradeComment");
						elTC.setAttribute("languageId", "505062");
						elTC.appendChild(doc.createTextNode(trade_comment));
						elCFT.appendChild(elTC);
					}
				}
			}
			content=getDocumentAsXml(doc);
			content = content.replaceAll("type=\"Trade\">",
					"xsi:type=\"Trade\">");
		} catch (Exception e) {
			Starter.cfLogger.error("Exception is captured while generating ContentItem: ", e);
		}

		return content;


	}
	
	private ArrayList<Element> CreateTradeOrganisation(Document doc,Long commodity_flow_id,Long td_id, SDICursorRow[] cursorCTOI)
	{
		ArrayList<Element> CTOs= new ArrayList<Element>();
		
		for (int i=0; i<cursorCTOI.length;i++)
		{
			SDICursorRow singleRow=cursorCTOI[i];
			
			Element TO=doc.createElement("TradeOrganisation");
			Long trade_id=singleRow.getLong("TRADE_ID");
			OracleObject trade_organisation_id=new OracleObject(OracleObject.RELATION_OBJECT_ID_TYPE, singleRow.getObjects("ORGANISATION_ID"));
			Long organisation_internal_id=singleRow.getLong("ORGANISATION_INTERNAL_ID");
			String organisation_name=singleRow.getString("ORGANISATION_NAME");
			String organisation_role=singleRow.getString("ORGANISATION_ROLE");
			String buy_sell_indicator=singleRow.getString("BUY_SELL_INDICATOR");
			Integer organisation_rank=singleRow.getInteger("ORGANISATION_RANK");
			
			if (!trade_id.equals(td_id))
			{
				continue;
			}
				
			if (trade_organisation_id !=null)
			{
				if (trade_organisation_id.getLong("object_id")!=null)
				{
					Element elTOI=CreateRelationObject(doc,commodity_flow_id,trade_organisation_id, "TradeOrganisationID");			
					TO.appendChild(elTOI);
				}
			}
			
			if (organisation_internal_id!=null)
			{
				Element elTOII=doc.createElement("TradeOrganisationIntID");
				elTOII.appendChild(doc.createTextNode(String.valueOf(organisation_internal_id)));
				TO.appendChild(elTOII);
			}			
			
			if (organisation_name != null)
			{	
				Element elTON=doc.createElement("TradeOrganisationName");
				elTON.setAttribute("languageId", "505062");
				elTON.appendChild(doc.createTextNode(organisation_name));
				TO.appendChild(elTON);
			}
			
			if (organisation_role != null)
			{	
				Element elTOR=doc.createElement("TradeOrganisationRole");
				elTOR.setAttribute("languageId", "505062");
				elTOR.appendChild(doc.createTextNode(organisation_role));
				TO.appendChild(elTOR);
			}
			
			if (buy_sell_indicator != null)
			{	
				Element elBSI=doc.createElement("TradeBuySell");
				elBSI.appendChild(doc.createTextNode(buy_sell_indicator));
				TO.appendChild(elBSI);
			}
			
			if (organisation_rank != null)
			{
				Element elTOR = doc.createElement("TradeOrganisationRank");
				elTOR.appendChild(doc.createTextNode(String.valueOf(organisation_rank)));
				TO.appendChild(elTOR);
			}
			
			CTOs.add(TO);
			
		}
		
		return CTOs;
	}

	@Override
	protected String getFooter() {
		// TODO Auto-generated method stub
		return "		</env:Body>\r\n" + "</env:ContentEnvelope>";
	}

	@Override
	protected String getHeader() {
		// TODO Auto-generated method stub
		String envolope = "<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n"
				+ "<env:ContentEnvelope xmlns:env=\"http://data.schemas.tfn.thomson.com/Envelope/2008-05-01/\" xmlns=\"http://CommodityFlows.schemas.financial.thomsonreuters.com/2012-05-30/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://CommodityFlows.schemas.financial.thomsonreuters.com/2012-05-30/ Trade.xsd\"  pubStyle=\"@PubType@\" majVers=\"3\" minVers=\"0.0\">\r\n";

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

}
