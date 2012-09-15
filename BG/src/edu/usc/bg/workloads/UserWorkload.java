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
import com.yahoo.ycsb.generator.CounterGenerator;
/**
 * Used for loading members into the data store
 * @author barahman
 *
 */
public class UserWorkload extends Workload {

	// The name of the table to insert records.
	public final static String table = "users";
	// The number of fields in a record.
	public final static int fieldCount = 11;
	// The length of each field for a record in Byte.
	public final static int fieldLength = 100;
	// The name of each field for the "user" table.
	public final static String[] fieldName = {"userid", "username", "pw", "fname", "lname", "gender",
		"dob", "jdate", "ldate", "address", "email", "tel"};
	// The base number for generating the random date.
	public final static long MAX_INTERVAL = 50000000000L;
	// The start of the date for generating the random date.
	public final static long BASE_INTERVAL = 1250000000000L;

	// These following fields could be kept in the property file.
	public static boolean insertImage = false;
	public static int imageSize = 1;  //1 is 2kb, 2 is 12 kb and 3 is 512 kb
	
	// The number of records to be inserted.
	int recordCount = 100; // User number.
	CounterGenerator keySequence;
	Vector<Integer> _members;
	Random random = new Random();

	public UserWorkload() {

	}

	// Initialize all of the threads with the same configuration.
	public void init(Properties p,  Vector<Integer> members) throws WorkloadException {
		recordCount=Integer.parseInt(p.getProperty(Client.USER_COUNT_PROPERTY, Client.USER_COUNT_PROPERTY_DEFAULT));
		keySequence = new CounterGenerator(0); // For generating user ID.
		insertImage = Boolean.parseBoolean(p.getProperty(Client.INSERT_IMAGE_PROPERTY, Client.INSERT_IMAGE_PROPERTY_DEFAULT));
		_members = members;
		if(p.getProperty(Client.IMAGE_SIZE_PROPERTY) != null)
			imageSize = Integer.parseInt(p.getProperty(Client.IMAGE_SIZE_PROPERTY));
		return;
	}


	// Return a date using the specific format.
	public String getDate(){
		Date date = new Date(random.nextLong()%MAX_INTERVAL + BASE_INTERVAL);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		String dateString = sdf.format(date);
		return dateString;
	}

	// Prepare the record for "user" table.
	// userid, username, pw, fname, lname, gender,
	// dob, jdate, ldate, address, email, tel.
	private LinkedHashMap<String, ByteIterator> buildValues() {
		LinkedHashMap<String, ByteIterator> values = new LinkedHashMap<String, ByteIterator>();

		for (int i = 1; i <= fieldCount; ++i)
		{
			// Generate the fields using StringByteIterator and RandomByteIterator.
			String fieldKey = fieldName[i];
			ByteIterator data;
			if(6 == i || 7 == i || 8 == 1 ){ // Date of birth, last login date, join date
				data = new StringByteIterator(getDate()); 
			}else{
				data = new RandomByteIterator(100);
			}
			values.put(fieldKey, data);
		} 	
		return values;
	}

	// Prepare the primary key for members
	public String buildKeyName(long keyNum) {
		String keyNumStr = "" + keyNum;
		return keyNumStr;
	}

	@Override
	public boolean doInsert(DB db, Object threadState) {
		int keyIdx = keySequence.nextInt();
		String dbKey = buildKeyName(_members.get(keyIdx));
		LinkedHashMap<String, ByteIterator> values = buildValues();
		if (db.insert(table, dbKey, values, insertImage, imageSize) == 0){
			return true;
		}else{
			return false;
		}
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
