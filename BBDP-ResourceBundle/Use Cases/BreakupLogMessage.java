/** File provided by V2 Maestros for its students for learning purposes only
 * Copyright @2016 All rights reserved.
 */
package com.v2maestros.bbdp.pig;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataType;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class BreakupLogMessage extends EvalFunc<Tuple> {

	static SimpleDateFormat inputFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
	static SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	@Override
	public Schema outputSchema(Schema input) {
		Schema tupleSchema = new Schema();
		
		tupleSchema.add(new FieldSchema("clientIp", DataType.CHARARRAY));
		tupleSchema.add(new FieldSchema("logTimestamp", DataType.CHARARRAY));
		tupleSchema.add(new FieldSchema("action", DataType.CHARARRAY));
		tupleSchema.add(new FieldSchema("url", DataType.CHARARRAY));
		tupleSchema.add(new FieldSchema("responseCode", DataType.INTEGER ));
		tupleSchema.add(new FieldSchema("responseSize", DataType.INTEGER));
		
		 Schema s = new Schema(new FieldSchema(null, tupleSchema));
		 return s;
	}

	@Override
	public Tuple exec(Tuple logTuple) throws IOException {
		
		String logStr = (String)logTuple.get(0);
		Map<String,String> logParts = getLogParts(logStr);
		
		Tuple outputTuple = TupleFactory.getInstance().newTuple(6);
		outputTuple.set(0, logParts.get("clientIp"));
		outputTuple.set(1,  logParts.get("logTimestamp"));
		outputTuple.set(2, logParts.get("action"));
		outputTuple.set(3, logParts.get("url"));
		outputTuple.set(4, logParts.get("responseCode"));
		outputTuple.set(5, logParts.get("responseSize"));
		
		return outputTuple;
	}
	
	private static Map<String,String> getLogParts(String logStr) {
		
		Map<String, String> logMap = new HashMap<String,String>();
		
		try {
			
			String[] parts = logStr.split(" ");
			for ( String part: parts) {
				System.out.println(part);
			}
			
			logMap.put("clientIp", parts[0]);
			
			Date logDate = inputFormat.parse(parts[3].replaceAll("\\[", ""));
			logMap.put("logTimestamp", outputFormat.format(logDate));
			
			logMap.put("action", parts[5].replaceAll("\"", ""));
			logMap.put("url", parts[6]);
			logMap.put("responseCode", parts[8]);
			logMap.put("responseSize", parts[9]);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return logMap;
		
	}

	//For testing purposes
	public static void main(String[] args) {
		String input="64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] \"GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1\" 401 12846";
		Map<String,String> logParts = getLogParts(input);
		System.out.println(logParts.toString());
	}
}
