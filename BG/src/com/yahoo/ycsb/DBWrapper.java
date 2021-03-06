/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
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

package com.yahoo.ycsb;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import edu.usc.bg.measurements.MyMeasurement;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 */
public class DBWrapper extends DB
{
	DB _db;
	MyMeasurement _measurements;

	public DBWrapper(DB db)
	{
		_db=db;
		_measurements=MyMeasurement.getMeasurements(Double.parseDouble(getProperties().getProperty(Client.EXPECTED_LATENCY_PROPERTY, Client.EXPECTED_LATENCY_PROPERTY_DEFAULT)));
	}

	/**
	 * Set the properties for this DB.
	 */
	public void setProperties(Properties p)
	{
		_db.setProperties(p);
	}

	/**
	 * Get the set of properties for this DB.
	 */
	public Properties getProperties()
	{
		return _db.getProperties();
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void init() throws DBException
	{
		_db.init();
	}

	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup(boolean warmup) throws DBException
	{
		_db.cleanup(warmup);
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @param insertImage identifies if images need to be inserted
	 * @param imageSize , identifies the size of image to be inserted for users
	 * @return Zero on success, a non-zero error code on error
	 */
	public int insert(String table, String key, HashMap<String,ByteIterator> values, boolean insertImage, int imageSize)
	{
		long st=System.nanoTime();
		int res=_db.insert(table,key,values, insertImage, imageSize);
		long en=System.nanoTime();
		_measurements.measure("INSERT",(int)((en-st)/1000));
		_measurements.reportReturnCode("INSERT",res);
		return res;
	}

	@Override
	public int getUserProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {
		//int res = 0;
		long st=System.nanoTime();
		int res=_db.getUserProfile(requesterID, profileOwnerID, result, insertImage, testMode);
		long en=System.nanoTime();
		_measurements.measure("PROFILE",(int)((en-st)/1000));
		_measurements.reportReturnCode("PROFILE",res);
		return res;
	}

	@Override
	public int getListOfFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,  boolean insertImage, boolean testMode) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.getListOfFriends(requesterID, profileOwnerID, fields, result, insertImage, testMode);
		long en=System.nanoTime();
		_measurements.measure("FRIENDS",(int)((en-st)/1000));
		_measurements.reportReturnCode("FRIENDS",res);
		return res;
	}

	@Override
	public int viewPendingRequests(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> values,  boolean insertImage, boolean testMode) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.viewPendingRequests(profileOwnerID, values,  insertImage, testMode);
		long en=System.nanoTime();
		_measurements.measure("PENDING",(int)((en-st)/1000));
		_measurements.reportReturnCode("PENDING",res);
		return res;
	}

	@Override
	public int acceptFriendRequest(int invitorID, int inviteeID) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.acceptFriendRequest(invitorID, inviteeID);
		long en=System.nanoTime();
		_measurements.measure("ACCEPT",(int)((en-st)/1000));
		_measurements.reportReturnCode("ACCEPT",res);
		return res;
	}

	@Override
	public int rejectFriendRequest(int invitorID, int inviteeID) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.rejectFriendRequest(invitorID, inviteeID);
		long en=System.nanoTime();
		_measurements.measure("REJECT",(int)((en-st)/1000));
		_measurements.reportReturnCode("REJECT",res);
		return res;
	}

	@Override
	public int inviteFriends(int invitorID, int inviteeID) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.inviteFriends(invitorID, inviteeID);
		long en=System.nanoTime();
		_measurements.measure("INV",(int)((en-st)/1000));
		_measurements.reportReturnCode("INV",res);
		return res;
	}

	@Override
	public int unFriendFriend(int friendid1, int friendid2) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.unFriendFriend(friendid1, friendid2);
		long en=System.nanoTime();
		_measurements.measure("UNFRIEND",(int)((en-st)/1000));
		_measurements.reportReturnCode("UNFRIEND",res);
		return res;
	}

	@Override
	public int getTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.getTopKResources(requesterID, profileOwnerID, k, result);
		long en=System.nanoTime();
		_measurements.measure("GETTOPRES",(int)((en-st)/1000));
		_measurements.reportReturnCode("GETTOPRES",res);
		return res;	}

	@Override
	public int getResourceComments(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.getResourceComments(requesterID, profileOwnerID, resourceID, result);
		long en=System.nanoTime();
		_measurements.measure("GETRESCOMMENT",(int)((en-st)/1000));
		_measurements.reportReturnCode("GETRESCOMMENT",res);
		return res;	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.postCommentOnResource(commentCreatorID, profileOwnerID, resourceID);
		long en=System.nanoTime();
		_measurements.measure("POSTCOMMENT",(int)((en-st)/1000));
		_measurements.reportReturnCode("POSTCOMMENT",res);
		return res;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		HashMap<String, String > stats = new HashMap<String, String>();
		stats =_db.getInitialStats();
		return stats;
	}

	public int CreateFriendship(int memberA, int memberB){
		long st=System.nanoTime();
		int res=_db.CreateFriendship(memberA, memberB);
		long en=System.nanoTime();
		_measurements.measure("CREATEFRIENDSHIP",(int)((en-st)/1000));
		_measurements.reportReturnCode("CREATEFRIENDSHIP",res);
		return res;
	}

	@Override
	public void createSchema(Properties props) {	
		_db.createSchema(props);
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		int res = _db.getCreatedResources(creatorID, result);
		return res;
	}

	@Override
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {
		int res = _db.queryPendingFriendshipIds(memberID, pendingIds);
		return res;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID,
			Vector<Integer> confirmedIds) {
		int res = _db.queryConfirmedFriendshipIds(memberID, confirmedIds);
		return res;
	}
	
	public void buildIndexes(Properties props){
		_db.buildIndexes(props);
	}
}
