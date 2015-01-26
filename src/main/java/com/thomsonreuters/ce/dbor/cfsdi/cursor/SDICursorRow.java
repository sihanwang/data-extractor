package com.thomsonreuters.ce.dbor.cfsdi.cursor;

import java.io.Serializable;
import java.util.HashMap;
import java.sql.Date;
import java.sql.Timestamp;

import com.thomsonreuters.ce.dbor.cfsdi.Starter;


public class SDICursorRow implements Serializable{
	
	private HashMap<String, Object> RowValues=new HashMap<String, Object>();
	
	public SDICursorRow(HashMap<String, Object> RowValues)
	{
		this.RowValues=RowValues;
	}
	
	public long getPerm_ID()
	{
		return ((java.math.BigDecimal)RowValues.get("COMMODITY_FLOW_ID")).longValue();
	}
	
	private Object getColumnValue(String column_name)
	{
		if (!RowValues.containsKey(column_name))
		{
			Starter.cfLogger.warn( "Invalid column name: "+ column_name);
			return null;
		}
		return RowValues.get(column_name);
	}
	
	public Object getObject(String column_name)
	{
		Object value=getColumnValue(column_name);
		
		return value;
		
	}
	
	public Long getLong(String column_name)
	{
		Object value=getColumnValue(column_name);
		if (value!=null)
		{
			return ((java.math.BigDecimal)value).longValue();
		}
			
		return null;
	}
	
	public Integer getInteger(String column_name)
	{
		Object value=getColumnValue(column_name);
		if (value!=null)
		{
			return ((java.math.BigDecimal)value).intValue();
		}
			
		return null;
	}	
	
	public Float getFloat(String column_name)
	{
		Object value=getColumnValue(column_name);
		if (value!=null)
		{
			return ((java.math.BigDecimal)value).floatValue();
		}
			
		return null;
	}	
	

	public Timestamp getTimestamp(String column_name)
	{
		Object value=getColumnValue(column_name);
		if (value!=null)
		{
			return (Timestamp)value;
		}
			
		return null;
	}
	
	public String getString(String column_name)
	{
		Object value=getColumnValue(column_name);
		if (value!=null)
		{
			return (String)value;
		}
			
		return null;
	}
	
	public Object[] getObjects(String column_name)
	{
		Object value=getColumnValue(column_name);
		if (value!=null)
		{
			return (Object[])value;
		}
			
		return null;
	}
	
	
	
	
	
}
