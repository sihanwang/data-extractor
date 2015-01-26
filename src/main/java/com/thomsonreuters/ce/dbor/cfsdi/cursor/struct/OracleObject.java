package com.thomsonreuters.ce.dbor.cfsdi.cursor.struct;

import java.sql.Date;
import java.sql.Timestamp;

public class OracleObject {
	
	public static final String[] RELATION_OBJECT_ID_TYPE = {
		"object_id",
		"relation_object_type_id",
		"relation_object_type",
		"relationship_id",
		"relationship_type_id",
		"related_object_id",
		"related_object_type_id",
		"related_object_type",
		"relation_role",
		"relationship_type",
		"relationship_confidence",
		"relation_object_na_code",
		"related_object_order",
		"relation_object_order",
		"effective_from",
		"effective_to"};
	
	public static final String[] PERMANENT_ID_IDENTIFIER_TYPE = {
	    "object_id",
	    "object_type_id",
	    "object_type"	    
	};
		
	private String[] Attributes;
	private Object[] Values;
	
	public OracleObject(String[] Attributes, Object[] Values )
	{
		this.Attributes=Attributes;
		this.Values=Values;
	}
	
	private Object getValueObj(String column_name)
	{
		for(int i=0;1<Attributes.length;i++)
		{
			if (Attributes[i].equals(column_name))
			{
				return Values[i];
			}			
		}
		
		return null;
	}
	
	public Long getLong(String column_name)
	{
		Object value=getValueObj( column_name);
		if (value!=null)
		{
			return ((java.math.BigDecimal)value).longValue();
		}
			
		return null;
	}
	
	public Integer getInteger(String column_name)
	{
		Object value=getValueObj( column_name);
		if (value!=null)
		{
			return ((java.math.BigDecimal)value).intValue();
		}
			
		return null;
	}	
	
	public Float getFloat(String column_name)
	{
		Object value=getValueObj( column_name);
		if (value!=null)
		{
			return ((java.math.BigDecimal)value).floatValue();
		}
			
		return null;
	}	
	
	public Date getDate(String column_name)
	{
		Object value=getValueObj( column_name);
		if (value!=null)
		{
			return (Date)value;
		}
			
		return null;
	}
	
	public Timestamp getTimestamp(String column_name)
	{
		Object value=getValueObj( column_name);
		if (value!=null)
		{
			return (Timestamp)value;
		}
			
		return null;
	}
	
	public String getString(String column_name)
	{
		Object value=getValueObj( column_name);
		if (value!=null)
		{
			return (String)value;
		}
			
		return null;
	}

}
