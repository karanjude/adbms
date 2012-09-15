/**                                                                                                                                                                                
 * Copyright (c) 2012 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */


package edu.usc.bg.workloads;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.CounterGenerator;
/**
 * Used for loading comments for the resources created for the members
 * @author barahman
 *
 */
public class ManipulationWorkload extends Workload {

	// The name of the table to insert records.
	public static String table = "manipulation";
	// The number of fields in a record.
	public static int fieldCount = 6;
	// The length of each field for a record in Byte.
	public static int fieldLength = 100;
	// The name of each field for the "manipulation" table.
	public static String[] fieldName = {"mid", "creatorid", "rid", "modifierid", "timestamp", "type", "content"};
	// The base number for generating the random date.
	public final static long MAX_INTERVAL = 50000000000L;
	// The start of the date for generating the random date.
	public final static long BASE_INTERVAL = 1250000000000L;


	// These following fields could be kept in the property file.
	// The number of users.
	public static int userCount = 100;
	// The number of resources.
	public static int resourceCount = 1000;
	// The number of average resources per user.
	public static int avgResourceCount = 10;
	// The number of average manipulations per resource.
	public static int avgManipulationCount = 0;
	// The number of record to be inserted.
	public static int recordCount = resourceCount * avgManipulationCount; // Manipulation number.


	private static IntegerGenerator keySequence ;
	private static IntegerGenerator creatorSequence;
	private static IntegerGenerator resourceSequence;
	private static int creatorNum  ;
	private static int resourceNum ;
	private static Random random = new Random();
	Vector<Integer> _members;

	public ManipulationWorkload() {

	}

	// Initialize all of the threads with the same configuration.
	public void init(Properties p,  Vector<Integer> members) throws WorkloadException {
		userCount=Integer.parseInt(p.getProperty(Client.USER_COUNT_PROPERTY));
		avgResourceCount=Integer.parseInt(p.getProperty(Client.RESOURCE_COUNT_PROPERTY));
		avgManipulationCount=Integer.parseInt(p.getProperty(Client.MANIPULATION_COUNT_PROPERTY));
		recordCount = resourceCount * avgManipulationCount; // Manipulation number.
		keySequence = new CounterGenerator(recordCount); // For generating manipulation ID.
		creatorSequence = new CounterGenerator(0); // For generating creator ID.
		resourceSequence = new CounterGenerator(0); // For generating resource ID.
		creatorNum  = creatorSequence.nextInt(); // The ID of creator
		resourceNum = resourceSequence.nextInt();
		_members = members;
		return;
	}


	// Return a date using the specific format.
	public static String getDate(){
		Date date = new Date(random.nextLong()%MAX_INTERVAL + BASE_INTERVAL);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		String dateString = sdf.format(date);

		return dateString;
	}

	// Prepare the record for "manipulation" table.
	// mid, creatorId, rid, modifiedId, timestamp, type, content.
	private LinkedHashMap<String, ByteIterator> buildValues(String dbKey, int creatorNum, int resourceNum) {
		LinkedHashMap<String, ByteIterator> values = new LinkedHashMap<String, ByteIterator>();

		for (int i = 1; i <= fieldCount; ++i)
		{
			// Generate the fields using StringByteIterator and RandomByteIterator.
			String fieldKey = fieldName[i];
			ByteIterator data;
			if(1 == i){
				data = new StringByteIterator(Integer.toString(creatorNum)); // Creator ID.
			}else if(2 == i){
				data = new StringByteIterator(Integer.toString(resourceNum)); 	// Resource ID.
			}else if(3 == i){
				data = new StringByteIterator(Integer.toString(userCount + random.nextInt(userCount))); // Modifier ID.
			}else if(4 == i){
				data = new StringByteIterator(getDate()); // Timestamp.
			}else{
				data = new RandomByteIterator(100); // Other fields.
			}
			values.put(fieldKey, data);
		}

		return values;
	}

	// Prepare the primary key.
	public String buildKeyName(long keyNum) {
		String keyNumStr = "" + keyNum;
		return keyNumStr;
	}

	private static int resourceCounter  = -1; // Counter for controlling the resources.
	private static int manipulationCounter = -1; // Counter for controlling the manipulations.
	@Override
	public boolean doInsert(DB db, Object threadState) {
		int keyNum = keySequence.nextInt(); // The ID of the manipulation.

		creatorNum = _members.get(creatorNum);
		resourceNum = creatorNum*avgResourceCount+resourceNum;

		if(++manipulationCounter == avgManipulationCount){
			if(++resourceCounter == avgResourceCount){
				creatorNum = creatorSequence.nextInt(); // The ID of the creator.
				creatorNum = _members.get(creatorNum);
				resourceSequence = new CounterGenerator(0); // For generating resource ID.
				resourceCounter = 0;
			}
			resourceNum = resourceSequence.nextInt(); // The ID of the resource.
			resourceNum = creatorNum*avgResourceCount+resourceNum;
			manipulationCounter = 0;
		}
		String dbKey = buildKeyName(keyNum);
		LinkedHashMap<String, ByteIterator> values = buildValues(dbKey, creatorNum, resourceNum);
		//manipulations dont have images so insertImage = false
		if (db.insert(table, dbKey, values, false,0) == 0)
			return true;
		else
			return false;
	}



	@Override
	public HashMap<String, String> getDBInitialStats(DB db) {
		HashMap<String, String> stats = new HashMap<String, String>();
		stats = db.getInitialStats();
		return stats;
	}

	@Override
	public int doTransaction(DB db, Object threadstate, int threadid,
			StringBuilder updateLog, StringBuilder readLog, int seqID,
			HashMap<String, Integer> resUpdateOperations,
			HashMap<String, Integer> frienshipInfo,
			HashMap<String, Integer> pendingInfo, int thinkTime,
			boolean insertImage, boolean warmup) {
		// TODO Auto-generated method stub
		return 0;
	}








}
