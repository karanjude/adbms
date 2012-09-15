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

/**
 * A layer for accessing a database to be benchmarked. Each thread in the client
 * will be given its own instance of whatever DB class is to be used in the test.
 * This class should be constructed using a no-argument constructor, so we can
 * load it dynamically. Any argument-based initialization should be
 * done by init().
 * 
 * Note that BG does not make any use of the return codes returned by this class.
 * Instead, it keeps a count of the return values and presents them to the user.
 * 
 * The semantics of methods vary from database  * to database.  
 */
public abstract class DB
{
	/**
	 * Properties for configuring this DB.
	 */
	Properties _p=new Properties();

	/**
	 * Set the properties for this DB.
	 */
	public void setProperties(Properties p)
	{
		_p=p;

	}

	/**
	 * Get the set of properties for this DB.
	 */
	public Properties getProperties()
	{
		return _p; 
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void init() throws DBException
	{
	}

	/**
	 * Cleanup any state for this DB.
	 * @param warmup identifies if the thread calling it is in the warmup phase
	 * this is needed so at the end of the warmup phase the cache wont be restarted
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup(boolean warmup) throws DBException
	{
	}
	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written 
	 * into the record with the specified record key.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @param insertImage identifies if images should be inserted for users
	 * @param imageSize , 1 for thumbnail 2kb, 2 for 12kb and 3 for 512kb
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract int insert(String table, String key, HashMap<String,ByteIterator> values, boolean insertImage, int imageSize);

	
	/**
	 * Get the profile object for a user 
	 * @param requesterID , unique requester user's identifier
	 * @param profileOwnerID , unique profile owner's user identifier
	 * @param result , HashMap with all data  
	 * @param insertImage identifies if the users have images in the database
	 * @param testMode set to true means the functionCOmmandLine is being used and images need to be stored on the filesystem if they exist
	 * These fields are the different fields within the profile
	 * such as friend count, notification count, ettc
	 * @return 0 on success, a non-zero error code on error.  See this class's description for a discussion of error codes. 
	 * the user's basic details will be shown
	 * If the requester is the owner of the profile the friendcount,
	 * pending friend request count, resource count are shown.
	 * 
	 * if the requester is not the owner of the profile
	 * then only the friend count and resource count are shown
	 * 
	 * 
	 */  
	
	public abstract int getUserProfile( int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode);

	/**get the list of friends for a member
	 * @param requesterID , the unique identifier of the user who wants to view profile owners friends
	 * @param profileOwnerID , this is the id of the profile we want to find list of friends for
	 * @param fields , contains the col names required for each friend (what info needed for every friend)
	 * fields The list of fields to read, or null for all of them 
	 * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record(friend)
	 * @param insertImage identifies if the users have images in the database
	 * @param testMode set to true identifies that the images should be written to a file if any exist
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract int getListOfFriends(int requesterID, int profileOwnerID, Set<String> fields, Vector<HashMap<String,ByteIterator>> result,  boolean insertImage, boolean testMode);
	
	/**get the list of pending friend requests for a member
	 * These are the requests that are generated by the profile owner 
	 * but have not been accepted or rejected by the invitees
	 * @param profileOwnerID , the profile owner's unique identifier
	 * @param values , a vector with all the pending invitee records
	 * @param insertImage identifies if the users have images in the database
	 * @param testMode needed for the functionCommanLine , if set to true will right pic to file if any exist
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract int viewPendingRequests(int profileOwnerID, Vector<HashMap<String,ByteIterator>> values, boolean insertImage, boolean testMode);

	/**
	 * Accept a pending friend request 
	 * This action can only be done by the invitee
	 * @param invitorID , the unique identifier of the invitor
	 * @param inviteeID , the unique identifier of the invitee
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract int acceptFriendRequest(int invitorID, int inviteeID);
	
	/**
	 * Reject a pending friend request
	 * This action can only be done by the invitee
	 * @param invitorID , the unique identifier of the invitor
	 * @param inviteeID , the unique identifier of the invitee
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	
	public abstract int rejectFriendRequest(int invitorID, int inviteeID);

	/**
	 * Generate friend request
	 *
	 * @param invitorID , the unique identifier of the invitor
	 * @param inviteeID , the unique identifier of the invitee
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract int inviteFriends(int invitorID, int inviteeID);
	
	/**
	 * get the top k resources and all their details for a member
	 * @param requesterID , the unique identifier of the user who wants to view profile owners resources
	 * @param profileOwnerID , the profile owner's unique identifier
	 * @param k , the number of resources requested
	 * @param result , a vector of all the resource records, every record is a hashmap containing values (not the posts tho)
	 * for all columns of a record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	
	public abstract int getTopKResources(int requesterID, int profileOwnerID, int k, Vector<HashMap<String,ByteIterator>> result);
	
	/**
	 * get the  resources  created by a user 
	 * @param creatorID , the unique identifier of the user who created the resources
	 * @param result , a vector of all the resource records, every record is a hashmap containing values (not the posts tho)
	 * for all columns of a record
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	
	public abstract int getCreatedResources(int creatorID, Vector<HashMap<String,ByteIterator>> result);
	
	/**
	 * get the comments for a resource and all comment details
	 * @param requesterID , the unique identifier of the user who wants to view profile owners resources
	 * @param profileOwnerID , the profile owner's unique identifier
	 * @param resourceID , the resource's unique identifier
	 * @param result , a vector of all the comment records for a specific resource
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract int getResourceComments(int requesterID, int profileOwnerID, int resourceID, Vector<HashMap<String,ByteIterator>> result);
	
	
	/**
	 * post a comment on a specific resource
	 * @param commentCreatorID , the unique identifier of the user who is creating the comment
	 * @param profileOwnerID , the profile owner's unique identifier (owner of the resource)
	 * @param resourceID , the resource's unique identifier
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract int postCommentOnResource(int commentCreatorID, int profileOwnerID, int resourceID);

	/**
	 * unfriend a friend 
	 * @param friendid1 , the unique identifier of the person who wants to remove a friend
	 * @param friendid2 , the unique identifier of the friend to be removed
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract int unFriendFriend(int friendid1, int friendid2);
	
	/**
	 * return DB initial statistics
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract HashMap<String, String> getInitialStats();
	
	/**
	 * Creates a confirmed friendship between memberA and memberB
	 * @param memberA
	 * @param memberB
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract int CreateFriendship(int memberA, int memberB);
	
	/**
	 * Creates the data store schema which will then be populated in the load phase
	 * @param props
	 */
	public abstract void createSchema(Properties props);
	
	/**
	 * queries only the memberids within the initial pending friendships for memberID
	 * @param memberID
	 * @param pendingIds , is a vector of all the member ids that have created a friendship invitation for memberID
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds);
	
	/**
	 * queries only the memberids within the initial confirmed friendships for memberID
	 * @param memberID
	 * @param confirmedIds , is a vector of all the member ids that have confirmed friendship for memberID
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds);

	/**
	 * May be used with data stores that support creation of indexes after the load phase
	 * @param props
	 */
	public void buildIndexes(Properties props){
	}
	
}
