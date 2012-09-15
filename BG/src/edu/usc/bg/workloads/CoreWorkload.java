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


import java.util.Properties;
import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;

import edu.usc.bg.Member;
import edu.usc.bg.generator.DistOfAccess;
import edu.usc.bg.generator.Fragmentation;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;



/**
 * queries the initial stats of the data store
 * @author barahman
 *
 */
class initQueryThread extends Thread{
	//needed for only accept friendship
	DB _db;
	int[] _tMembers;
	Properties _props ;
	HashMap<Integer, Vector<Integer>> _pIds = new HashMap<Integer, Vector<Integer>>();
	HashMap<Integer,Vector<Integer>> _cIds = new HashMap<Integer, Vector<Integer>>();
	HashMap<String, Integer> _initCnt = new HashMap<String, Integer>();

	initQueryThread(int[] tMembers, Properties props){
		_tMembers = tMembers;
		//init DB
		String dbname = props.getProperty(Client.DB_CLIENT_PROPERTY, Client.DB_CLIENT_PROPERTY_DEFAULT);
		_props = props;
		try {
			_db = DBFactory.newDB(dbname, props);
			_db.init();
		} catch (UnknownDBException e) {
			System.out.println("Unknown DB, QpendingThread " + dbname);
			System.exit(0);
		} catch (DBException e) {
			e.printStackTrace(System.out);
			System.exit(0);
		}
	}

	public HashMap<String, Integer> getInit(){
		return _initCnt;
	}

	public HashMap<Integer, Vector<Integer>> getPendings(){
		return _pIds;
	}

	public HashMap<Integer, Vector<Integer>> getConfirmed(){
		return _cIds;
	}
	public void run(){
		int res =0;
		for(int i=0; i<_tMembers.length; i++){
			HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
			res = _db.getUserProfile(_tMembers[i], _tMembers[i], result, false, false);
			if(res < 0){
				System.out.println("Problem in getting initial stats.");
				System.exit(0);	
			}	
			_initCnt.put("PENDFRND-"+_tMembers[i], Integer.parseInt(result.get("PendingCount").toString().trim()));
			_initCnt.put("ACCEPTFRND-"+_tMembers[i], Integer.parseInt(result.get("FriendCount").toString().trim()));
			//get all resources for this user
			Vector<HashMap<String, ByteIterator>> resResult = new Vector<HashMap<String, ByteIterator>>();
			res = _db.getCreatedResources(_tMembers[i], resResult);
			if(res < 0){
				System.out.println("Problem in getting initial stats.");
				System.exit(0);	
			}	
			for(int d=0; d<resResult.size(); d++){
				String resId = resResult.get(d).get("RID").toString().trim();
				Vector<HashMap<String, ByteIterator>> commentResult = new Vector<HashMap<String, ByteIterator>>();
				res = _db.getResourceComments(_tMembers[i], _tMembers[i], Integer.parseInt(resId), commentResult);
				if(res < 0){
					System.out.println("Problem in getting initial stats.");
					System.exit(0);	
				}	
				_initCnt.put("POSTCOMMENT-"+resId, commentResult.size());
			}
			//get pending friends to relate them
			Vector<Integer> pids = new Vector<Integer>();
			res = _db.queryPendingFriendshipIds(_tMembers[i], pids);
			if(res < 0){
				System.out.println("Problem in getting initial stats.");
				System.exit(0);	
			}	
			_pIds.put(_tMembers[i], pids);
			//get confirmed friends to relate them
			Vector<Integer> cids = new Vector<Integer>();
			res = _db.queryConfirmedFriendshipIds(_tMembers[i], cids);
			if(res < 0){
				System.out.println("Problem in getting initial stats.");
				System.exit(0);	
			}	
			_cIds.put(_tMembers[i], cids);
		}

		try {
			_db.cleanup(true);
		} catch (Exception e) {
			e.printStackTrace(System.out);
			return;
		}
	}
}

/**
 * responsible for creating the benchmarking workload for issuing the queries based on the workload file specified
 * @author barahman
 *
 */
public class CoreWorkload extends Workload
{
	public static final boolean enableLogging = true;
	/**
	 * needed for zipfian distributions
	 */
	public static final String ZIPF_MEAN_PROPERTY= "zipfianmean";
	public static final String ZIPF_MEAN_PROPERTY_DEFAULT = "0.27";
	/**
	 * The name of the property for the the distribution of requests across the keyspace. Options are "uniform", "zipfian" and "latest"
	 */
	public static final String REQUEST_DISTRIBUTION_PROPERTY="requestdistribution";
	/**
	 * The default distribution of requests across the keyspace
	 */
	public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT="uniform";
	/**
	 * Percentage users that only access their own profile property name
	 */
	public static final String GETOWNPROFILE_PROPORTION_PROPERTY="BrowseSelfProfileSession";
	/**
	 * The default proportion of functionalities that are getOwnProfile
	 */
	public static final String GETOWNPROFILE_PROPORTION_PROPERTY_DEFAULT="1.0";
	/**
	 * Percentage users that  access their friend profile property name
	 */
	public static final String GETFRIENDPROFILE_PROPORTION_PROPERTY="BrowseFriendProfileSession";
	/**
	 * The default proportion of functionalities that are getFriendProfile
	 */
	public static final String GETFRIENDPROFILE_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that  post comment on their own resource
	 */
	public static final String POSTCOMMENTONRESOURCE_PROPORTION_PROPERTY="CommentOnFriendResourceSession";
	/**
	 * The default proportion of functionalities that are postCommentOnResource
	 */
	public static final String POSTCOMMENTONRESOURCE_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that  do the generate friendship requests in their sessions
	 */
	public static final String GENERATEFRIENDSHIP_PROPORTION_PROPERTY="GenerateFriendReqSession";
	/**
	 * The default proportion of functionalities that are generateFriendRequest
	 */
	public static final String GENERATEFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT="0.0";	
	/**
	 * Percentage users that  do the accept friendship sequence
	 */
	public static final String ACCEPTFRIENDSHIP_PROPORTION_PROPERTY="AcceptFriendReqSession";
	/**
	 * The default proportion of functionalities that are acceptFriendship
	 */
	public static final String ACCEPTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the reject friendship sequence
	 */
	public static final String REJECTFRIENDSHIP_PROPORTION_PROPERTY="RejectFriendReqSession";
	/**
	 * The default proportion of functionalities that are rejectFriendship
	 */
	public static final String REJECTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the unfriend friendship user session
	 */
	public static final String UNFRIEND_PROPORTION_PROPERTY="ThawFriendshipSession";
	/**
	 * The default proportion of functionalities that are unFriendFriend
	 */
	public static final String UNFRIEND_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the unfriend/accept friendship sequence
	 */
	public static final String UNFRIENDACCEPTFRIENDSHIP_PROPORTION_PROPERTY="unFriendAcceptProportion";
	/**
	 * The default proportion of functionalities that are unfriend/accept
	 */
	public static final String UNFRIENDACCEPTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the unfriend/reject friendship sequence
	 */
	public static final String UNFRIENDREJECTFRIENDSHIP_PROPORTION_PROPERTY="unFriendRejectProportion";
	/**
	 * The default proportion of functionalities that are unfriend/reject
	 */
	public static final String UNFRIENDREJECTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT="0.0";

	//individual actions
	/**
	 * Percentage users that do the getProfile action
	 */
	public static final String GETRANDOMPROFILEACTION_PROPORTION_PROPERTY="ViewProfileAction";
	/**
	 * The default proportion of getprofile action
	 */
	public static final String GETRANDOMPROFILEACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the getlistoffriends action
	 */
	public static final String GETRANDOMLISTOFFRIENDSACTION_PROPORTION_PROPERTY="ListFriendsAction";
	/**
	 * The default proportion of getlistoffriends action
	 */
	public static final String GETRANDOMLISTOFFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the getlistofpendingrequests action
	 */
	public static final String GETRANDOMLISTOFPENDINGREQUESTSACTION_PROPORTION_PROPERTY="ViewFriendReqAction";
	/**
	 * The default proportion of getlistofpendingrequests action
	 */
	public static final String GETRANDOMLISTOFPENDINGREQUESTSACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the invitefriends action
	 */
	public static final String INVITEFRIENDSACTION_PROPORTION_PROPERTY="InviteFriendAction";
	/**
	 * The default proportion of invitefriends action
	 */
	public static final String INVITEFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the acceptfriends action
	 */
	public static final String ACCEPTFRIENDSACTION_PROPORTION_PROPERTY="AcceptFriendReqAction";
	/**
	 * The default proportion of acceptfriends action
	 */
	public static final String ACCEPTFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the rejectfriends action
	 */
	public static final String REJECTFRIENDSACTION_PROPORTION_PROPERTY="RejectFriendReqAction";
	/**
	 * The default proportion of rejectfriends action
	 */
	public static final String REJECTFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the unfriendfriends action
	 */
	public static final String UNFRIENDFRIENDSACTION_PROPORTION_PROPERTY="ThawFriendshipAction";
	/**
	 * The default proportion of unfriendfriends action
	 */
	public static final String UNFRIENDFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the gettopresources action
	 */
	public static final String GETTOPRESOURCEACTION_PROPORTION_PROPERTY="ViewTopKResourcesAction";
	/**
	 * The default proportion of gettopresources action
	 */
	public static final String GETTOPRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the getcommentsonresources action
	 */
	public static final String GETCOMMENTSONRESOURCEACTION_PROPORTION_PROPERTY="ViewCommentsOnResourceAction";
	/**
	 * The default proportion of getcommentsonresources action
	 */
	public static final String GETCOMMENTSONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT="0.0";
	/**
	 * Percentage users that do the postcommentonresources action
	 */
	public static final String POSTCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY="PostCommentOnResourceAction";
	/**
	 * The default proportion of postcommentonresources action
	 */
	public static final String POSTCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT="0.0";

	//if no reads have occurred or no read log files created the validation check need not happen-shared for all threads
	public static boolean readsExist = false;
	//if no updates have occurred or no update log files have been created the validation check need not happen-shared for all threads 
	public static boolean updatesExist = false;
	
	public static HashMap<String, Integer> initStats = new HashMap<String, Integer>();

	private static int numShards = 101;
	private static char[][] userStatusShards;
	private static Semaphore[] uStatSemaphores;
	//keeps a track of user frequency of access
	private static int[][] userFreqShards;
	private static Semaphore[] uFreqSemaphores;
	private static DistOfAccess myDist;
	private static Member[] myMemberObjs;	

	static Random random = new Random();
	IntegerGenerator keysequence;
	CounterGenerator transactioninsertkeysequence;
	DiscreteGenerator operationchooser;
	IntegerGenerator keychooser;
	int usercount;
	int useroffset;
	//a for active and d for deactive
	
	//keep a track of all related users for every user
	private HashMap<Integer,HashMap<Integer, String>> userRelations = null;
	private static Semaphore rStat = new Semaphore(1, true);
	
	//needed only for the accept friendship workload
	//no need to have a lock coz a user cant be active by multiple threads to accept invitations
	private static HashMap<Integer, Vector<Integer>> pendingFrnds = new HashMap<Integer, Vector<Integer>>();
	private static Semaphore aFrnds = new Semaphore(1, true);
	private static HashMap<Integer, HashMap<Integer, String>> acceptedFrnds = new HashMap<Integer, HashMap<Integer, String>>();
	int[] myMembers;
	HashMap<Integer, Integer> memberIdxs = new HashMap<Integer, Integer>();

	String requestdistrib = "";
	int machineid = 0;
	int numBGClients = 1;
	double ZipfianMean = 0.27;


	/**
	 * Initialize the scenario. 
	 * Called once, in the main client thread, before any operations are started.
	 */
	public void init(Properties p, Vector<Integer> members) throws WorkloadException
	{	

		//sessions
		double getownprofileproportion=Double.parseDouble(p.getProperty(GETOWNPROFILE_PROPORTION_PROPERTY,GETOWNPROFILE_PROPORTION_PROPERTY_DEFAULT));
		double getfriendprofileproportion=Double.parseDouble(p.getProperty(GETFRIENDPROFILE_PROPORTION_PROPERTY,GETFRIENDPROFILE_PROPORTION_PROPERTY_DEFAULT));
		double postcommentonresourceproportion=Double.parseDouble(p.getProperty(POSTCOMMENTONRESOURCE_PROPORTION_PROPERTY,POSTCOMMENTONRESOURCE_PROPORTION_PROPERTY_DEFAULT));
		double acceptfriendshipproportion=Double.parseDouble(p.getProperty(ACCEPTFRIENDSHIP_PROPORTION_PROPERTY,ACCEPTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT));
		double genfriendshipproportion=Double.parseDouble(p.getProperty(GENERATEFRIENDSHIP_PROPORTION_PROPERTY,GENERATEFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT));
		double rejectfriendshipproportion=Double.parseDouble(p.getProperty(REJECTFRIENDSHIP_PROPORTION_PROPERTY,REJECTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT));
		double unfriendfriendproportion=Double.parseDouble(p.getProperty(UNFRIEND_PROPORTION_PROPERTY,UNFRIEND_PROPORTION_PROPERTY_DEFAULT));
		double unfriendacceptfriendshipproportion=Double.parseDouble(p.getProperty(UNFRIENDACCEPTFRIENDSHIP_PROPORTION_PROPERTY,UNFRIENDACCEPTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT));
		double unfriendrejectfriendshipproportion=Double.parseDouble(p.getProperty(UNFRIENDREJECTFRIENDSHIP_PROPORTION_PROPERTY,UNFRIENDREJECTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT));
		//actions
		double getprofileactionproportion = Double.parseDouble(p.getProperty(GETRANDOMPROFILEACTION_PROPORTION_PROPERTY,GETRANDOMPROFILEACTION_PROPORTION_PROPERTY_DEFAULT));
		double getfriendsactionproportion = Double.parseDouble(p.getProperty(GETRANDOMLISTOFFRIENDSACTION_PROPORTION_PROPERTY,GETRANDOMLISTOFFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT));
		double getpendingrequestsactionproportion = Double.parseDouble(p.getProperty(GETRANDOMLISTOFPENDINGREQUESTSACTION_PROPORTION_PROPERTY,GETRANDOMLISTOFPENDINGREQUESTSACTION_PROPORTION_PROPERTY_DEFAULT));
		double invitefriendsactionproportion = Double.parseDouble(p.getProperty(INVITEFRIENDSACTION_PROPORTION_PROPERTY,INVITEFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT));
		double acceptfriendactionproportion = Double.parseDouble(p.getProperty(ACCEPTFRIENDSACTION_PROPORTION_PROPERTY,ACCEPTFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT));
		System.out.println(acceptfriendactionproportion);
		double rejectfriendactionproportion = Double.parseDouble(p.getProperty(REJECTFRIENDSACTION_PROPORTION_PROPERTY,REJECTFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT));
		double unfriendfriendactionproportion = Double.parseDouble(p.getProperty(UNFRIENDFRIENDSACTION_PROPORTION_PROPERTY,UNFRIENDFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT));
		double gettopresourcesactionproportion = Double.parseDouble(p.getProperty(GETTOPRESOURCEACTION_PROPORTION_PROPERTY,GETTOPRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT));
		double postcommentonresourceactionproportion = Double.parseDouble(p.getProperty(POSTCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY,POSTCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT));
		double getresourcecommentsactionproportion = Double.parseDouble(p.getProperty(GETCOMMENTSONRESOURCEACTION_PROPORTION_PROPERTY,GETCOMMENTSONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT));

		usercount=Integer.parseInt(p.getProperty(Client.USER_COUNT_PROPERTY, Client.USER_COUNT_PROPERTY_DEFAULT));
		useroffset = Integer.parseInt(p.getProperty(Client.USER_OFFSET_PROPERTY,Client.USER_COUNT_PROPERTY_DEFAULT));
		requestdistrib=p.getProperty(REQUEST_DISTRIBUTION_PROPERTY,REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
		machineid = Client.machineid;
		
		
		keysequence=new CounterGenerator(useroffset);
		operationchooser=new DiscreteGenerator();
		if (getownprofileproportion>0)
		{
			operationchooser.addValue(getownprofileproportion,"OWNPROFILE");
		}

		if (getfriendprofileproportion>0)
		{
			operationchooser.addValue(getfriendprofileproportion,"FRIENDPROFILE");
		}

		if (postcommentonresourceproportion>0)
		{
			operationchooser.addValue(postcommentonresourceproportion,"POSTCOMMENT");
		}

		if(acceptfriendshipproportion > 0)
		{
			operationchooser.addValue(acceptfriendshipproportion,"ACCEPTREQ");
		}

		if(rejectfriendshipproportion > 0)
		{
			operationchooser.addValue(rejectfriendshipproportion,"REJECTREQ");
		}

		if(unfriendacceptfriendshipproportion > 0)
		{
			operationchooser.addValue(unfriendacceptfriendshipproportion,"UNFRNDACCEPTREQ");
		}

		if(unfriendrejectfriendshipproportion > 0)
		{
			operationchooser.addValue(unfriendrejectfriendshipproportion,"UNFRNDREJECTREQ");
		}

		if(unfriendfriendproportion > 0)
		{
			operationchooser.addValue(unfriendfriendproportion,"UNFRNDREQ");
		}

		if(genfriendshipproportion > 0)
		{
			operationchooser.addValue(genfriendshipproportion,"GENFRNDREQ");
		}

		//actions
		if(getprofileactionproportion > 0)
		{
			operationchooser.addValue(getprofileactionproportion,"GETPROACT");
		}
		if(getfriendsactionproportion > 0)
		{
			operationchooser.addValue(getfriendsactionproportion,"GETFRNDLSTACT");
		}
		if(getpendingrequestsactionproportion > 0)
		{
			operationchooser.addValue(getpendingrequestsactionproportion,"GETPENDACT");
		}
		if(invitefriendsactionproportion > 0)
		{
			operationchooser.addValue(invitefriendsactionproportion,"INVFRNDACT");
		}
		if(acceptfriendactionproportion > 0)
		{
			operationchooser.addValue(acceptfriendactionproportion,"ACCFRNDACT");
		}
		if(rejectfriendactionproportion > 0)
		{
			operationchooser.addValue(rejectfriendactionproportion,"REJFRNDACT");
		}
		if(unfriendfriendactionproportion > 0)
		{
			operationchooser.addValue(unfriendfriendactionproportion,"UNFRNDACT");
		}
		if(gettopresourcesactionproportion > 0)
		{
			operationchooser.addValue(gettopresourcesactionproportion,"GETRESACT");
		}
		if(getresourcecommentsactionproportion > 0)
		{
			operationchooser.addValue(getresourcecommentsactionproportion,"GETCMTACT");
		}
		if(postcommentonresourceactionproportion > 0)
		{
			operationchooser.addValue(postcommentonresourceactionproportion,"POSTCMTACT");
		}

		transactioninsertkeysequence=new CounterGenerator(usercount);
		long loadst = System.currentTimeMillis();
		if (requestdistrib.compareTo("uniform")==0)
		{
			keychooser=new UniformIntegerGenerator(0,usercount-1);
			myMembers = new int[usercount];
			myMemberObjs = new Member[usercount];
			for(int j=0; j<usercount; j++){
				myMembers[j] = j+useroffset;
				memberIdxs.put(j+useroffset, j);
				Member newMember = new Member(j+useroffset, j, (j%numShards), (j/numShards+j%numShards));
				myMemberObjs[j] = newMember;
			}

		}
		else if (requestdistrib.compareTo("zipfian")==0)
		{
			//it does this by generating a random "next key" in part by taking the modulus over the number of keys
			//if the number of keys changes, this would shift the modulus, and we don't want that to change which keys are popular
			//so we'll actually construct the scrambled zipfian generator with a keyspace that is larger than exists at the beginning
			//of the test. that is, we'll predict the number of inserts, and tell the scrambled zipfian generator the number of existing keys
			//plus the number of predicted keys as the total keyspace. then, if the generator picks a key that hasn't been inserted yet, will
			//just ignore it and pick another key. this way, the size of the keyspace doesn't change from the perspective of the scrambled zipfian generator
			keychooser=new ScrambledZipfianGenerator(usercount);
			myMembers = new int[usercount];
			myMemberObjs = new Member[usercount];
			for(int j=0; j<usercount; j++){
				myMembers[j] = j+useroffset;
				memberIdxs.put(j+useroffset, j);
				Member newMember = new Member(j+useroffset, j, (j%numShards), (j/numShards+j%numShards));
				myMemberObjs[j] = newMember;
			}
		}
		else if(requestdistrib.compareTo("dzipfian")==0){
			System.out.println("Create fragments in workload init phase");
			Fragmentation createFrags = new Fragmentation(usercount, Integer.parseInt(p.getProperty(Client.NUM_BG_PROPERTY,Client.NUM_BG_PROPERTY_DEFAULT)),machineid,p.getProperty("probs",""), ZipfianMean);
			myDist = createFrags.getMyDist();
			myMembers = createFrags.getMyMembers();
			myMemberObjs =new Member[myMembers.length];
			for(int j=0; j<myMembers.length;j++){
				memberIdxs.put(myMembers[j], j);
				Member newMember = new Member(myMembers[j], j, (j%numShards), (j/numShards));
				myMemberObjs[j] = newMember;
			}
			usercount = myMembers.length;
		}
		else if (requestdistrib.compareTo("latest")==0)
		{
			keychooser=new SkewedLatestGenerator(transactioninsertkeysequence);
			myMembers = new int[usercount];
			myMemberObjs = new Member[usercount];
			for(int j=0; j<usercount; j++){
				myMembers[j] = j+useroffset;
				memberIdxs.put(j+useroffset, j);
				Member newMember = new Member(j+useroffset, j, (j%numShards), (j/numShards+j%numShards));
				myMemberObjs[j] = newMember;
			}
		}
		else
		{
			throw new WorkloadException("Unknown request distribution \""+requestdistrib+"\"");
		}
		System.out.println("Time to create fragments : "+(System.currentTimeMillis()-loadst)+" msec");
		
		//init is called once and by the client so no need to lock
		//create the shards and their corresponding semaphores
		//initiate the user status to 'd' for user status
		//initiate to 0 for frequency
		userStatusShards = new char[numShards][];
		uStatSemaphores = new Semaphore[numShards];
		
		userFreqShards = new int[numShards][];
		uFreqSemaphores = new Semaphore[numShards];
		
		int avgShardSize = usercount/numShards;
		int remainingMembers = usercount-(avgShardSize*numShards);
		for(int i=0; i<numShards; i++){
			//determine the size of every shard
			int numShardUsers = avgShardSize;
			if(i<remainingMembers)
				numShardUsers++;
			userStatusShards[i] = new char[numShardUsers];
			userFreqShards[i] = new int[numShardUsers];
			for(int j=0; j<numShardUsers; j++){
				userStatusShards[i][j]='d';
				userFreqShards[i][j]=0;
			}
			uStatSemaphores[i] = new Semaphore(1, true);
			uFreqSemaphores[i] = new Semaphore(1, true);
		}
	
		
		
		try {
			rStat.acquire();
			userRelations = new HashMap<Integer,HashMap<Integer,String>>();;
			for(int i=0; i<myMembers.length; i++){
				//initially adding myself to the related vector
				HashMap<Integer, String> init = new HashMap<Integer, String>();
				init.put(myMembers[i],"");
				userRelations.put(myMembers[i], init);
			}
			rStat.release();
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		
		
		//initialize the pendingFrnds and the acceptedFrnds data structures
		for(int i=0; i<myMembers.length;i++){
			pendingFrnds.put(myMembers[i], new Vector<Integer>());
			acceptedFrnds.put(myMembers[i], new HashMap<Integer,String>());
		}
			
		if(p.getProperty(Client.INIT_STATS_REQ_APPROACH_PROPERTY) != null && p.getProperty(Client.INIT_STATS_REQ_APPROACH_PROPERTY).equalsIgnoreCase("QUERYDATA")){
			//query data store for pending and confirmed friends
			int numQThreads = 5;
			Vector<initQueryThread> qThreads = new Vector<initQueryThread>();
			int tUserCount = myMembers.length / numQThreads;
			int remainingUsers = myMembers.length - (tUserCount*numQThreads);
			int addUser = 0;
			for(int u=0; u<numQThreads; u++){
				if(u == numQThreads-1)
					addUser = remainingUsers;
				int[] tMembers = new int[tUserCount+addUser];
				for(int d=0; d<tUserCount+addUser; d++)
					tMembers[d] = myMembers[d+u*tUserCount];
				initQueryThread t = new initQueryThread(tMembers, p);
				qThreads.add(t);
				t.start();	
			}
			for (Thread t : qThreads) {
				try {
					t.join();
					initStats.putAll(((initQueryThread)t).getInit());
					HashMap<Integer, Vector<Integer>> pends = ((initQueryThread)t).getPendings();
					pendingFrnds.putAll(pends);
					Set<Integer> keys = pends.keySet();
					Iterator<Integer> it = keys.iterator();
					while(it.hasNext()){
						int aKey = (Integer)(it.next());
						for(int d=0; d<pends.get(aKey).size(); d++){
							relateUsers(aKey, pends.get(aKey).get(d));
						}
					}
					HashMap<Integer, Vector<Integer>> confs = ((initQueryThread)t).getConfirmed();
					keys = confs.keySet();
					it = keys.iterator();
					while(it.hasNext()){
						int aKey = (Integer)(it.next());
						for(int d=0; d<confs.get(aKey).size(); d++){
							acceptedFrnds.get(aKey).put(confs.get(aKey).get(d),"");
							relateUsers(aKey, confs.get(aKey).get(d));
						}
					}
				}catch(Exception e){
					e.printStackTrace(System.out);
				}
			}

			/*Set keys = initStats.keySet();
			Iterator it = keys.iterator();
			while(it.hasNext()){
				String key = (String)(it.next());
				System.out.println(key+" "+initStats.get(key));
			}*/
		}
		

	}

	/**
	 * Do one transaction operation. Because it will be called concurrently from multiple client threads, this 
	 * function must be thread safe. 
	 * returns the number of actions done
	 */
	public int doTransaction(DB db, Object threadstate, int threadid,  StringBuilder updateLog, StringBuilder readLog,  int seqID, HashMap<String, Integer> resUpdateOperations
			, HashMap<String, Integer> friendshipInfo, HashMap<String, Integer> pendingInfo, int thinkTime, boolean insertImage, boolean warmup)
	{
		String op=operationchooser.nextString();
		int opsDone = 0;

		if (op.compareTo("OWNPROFILE")==0)
		{
			opsDone = doTransactionOwnProfile(db, threadid, updateLog, readLog ,seqID, thinkTime, insertImage,  warmup);
		}
		else if (op.compareTo("FRIENDPROFILE")==0)
		{
			opsDone = doTransactionFriendProfile(db, threadid, updateLog, readLog,seqID, thinkTime,  insertImage, warmup);
		}
		else if (op.compareTo("POSTCOMMENT")==0)
		{
			opsDone = doTransactionPostCommentOnResource(db,threadid, updateLog, readLog,seqID, resUpdateOperations, thinkTime, insertImage, warmup);
		}
		else if (op.compareTo("ACCEPTREQ")==0)
		{
			opsDone = doTransactionAcceptFriendship(db,threadid,  updateLog, readLog,seqID, friendshipInfo, pendingInfo, thinkTime, insertImage, warmup);
		}
		else if (op.compareTo("REJECTREQ") == 0)
		{
			opsDone = doTransactionRejectFriendship(db,threadid,  updateLog,readLog ,seqID, friendshipInfo, pendingInfo, thinkTime, insertImage, warmup);
		}
		else if (op.compareTo("UNFRNDACCEPTREQ") == 0)
		{
			opsDone = doTransactionUnfriendPendingFriendship(db,threadid, updateLog,readLog, seqID, friendshipInfo, pendingInfo, thinkTime, "ACCEPT", insertImage, warmup);
		}
		else if (op.compareTo("UNFRNDREJECTREQ") == 0)
		{
			opsDone = doTransactionUnfriendPendingFriendship(db,threadid, updateLog, readLog,seqID, friendshipInfo, pendingInfo, thinkTime, "REJECT", insertImage, warmup);
		}
		else if (op.compareTo("UNFRNDREQ") == 0)
		{
			opsDone = doTransactionUnfriendFriendship(db,threadid,  updateLog, readLog,seqID, friendshipInfo, pendingInfo, thinkTime, insertImage, warmup);
		}
		else if (op.compareTo("GENFRNDREQ") == 0)
		{
			opsDone = doTransactionGenerateFriendship(db,threadid,  updateLog, readLog ,seqID, friendshipInfo, pendingInfo, thinkTime, insertImage, warmup);
		}
		//actions
		else if (op.compareTo("GETPROACT") == 0)
		{
			opsDone = doActionGetProfile(db, threadid, updateLog, readLog ,seqID, insertImage, warmup);
		}
		else if (op.compareTo("GETFRNDLSTACT") == 0)
		{
			opsDone = doActionGetFriends(db,threadid, updateLog,readLog,seqID, insertImage, warmup);
		}
		else if (op.compareTo("GETPENDACT") == 0)
		{
			opsDone = doActionGetPendings(db,threadid, updateLog,readLog,seqID, insertImage, warmup);
		}
		else if (op.compareTo("INVFRNDACT") == 0)
		{
			opsDone = doActionInviteFriends(db,threadid, updateLog,readLog, seqID, friendshipInfo, pendingInfo, insertImage, warmup);
		}
		else if (op.compareTo("ACCFRNDACT") == 0)
		{
			opsDone = doActionAcceptFriends(db, threadid, updateLog,readLog, seqID, friendshipInfo,pendingInfo, thinkTime,  insertImage,warmup);
		}
		else if (op.compareTo("REJFRNDACT") == 0)
		{
			opsDone = doActionRejectFriends(db, threadid, updateLog,readLog, seqID, friendshipInfo,pendingInfo, thinkTime,  insertImage,warmup);
		}
		else if (op.compareTo("UNFRNDACT") == 0)
		{
			opsDone = doActionUnFriendFriends(db, threadid,updateLog,readLog, seqID, friendshipInfo
					,pendingInfo, thinkTime,  insertImage, warmup);
		}
		else if (op.compareTo("GETRESACT") == 0)
		{
			opsDone = doActionGetTopResources(db, threadid, updateLog, readLog ,seqID, insertImage,  warmup);
		}
		else if (op.compareTo("GETCMTACT") == 0)
		{
			opsDone = doActionGetResourceComments(db, threadid, updateLog, readLog ,seqID, thinkTime, insertImage,  warmup);
		}
		else if (op.compareTo("POSTCMTACT") == 0)
		{
			opsDone = doActionPostComments(db,threadid, updateLog, readLog,seqID, resUpdateOperations,thinkTime, insertImage,  warmup);
		}

		return opsDone;
	}

	public int buildKeyName(int keynum) {

		//int key = useroffset+random.nextInt(keynum);
		int key =0;
		if(requestdistrib.compareTo("dzipfian")==0){
			int idx = myDist.GenerateOneItem()-1;
			//key = createFrags.getMemberIdForIdx(idx);
			key = myMembers[idx];

		}else
			key = keychooser.nextInt()+useroffset;

		return key;
	}


	public int doTransactionOwnProfile(DB db, int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, int thinkTime, boolean insertImage,  boolean warmup)
	{			
		int numOpsDone =0;
		int keyname = buildKeyName(usercount);
		//activate the user so no one else can grab it
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		//update frequency of access for the picked user
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.getUserProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}	
		long endReadp = System.nanoTime();

		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("FriendCount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("PendingCount")+"\n");
			readsExist = true;
		}

		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		ret = db.getTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		//view comments on one of your own resources
		if(rResult.size() > 0){
			Random random = new Random();
			int idx = random.nextInt(rResult.size());
			Vector<HashMap<String,ByteIterator>> cResult=new Vector<HashMap<String,ByteIterator>>();
			String resourceID ="";
			resourceID = rResult.get(idx).get("RID").toString();
			long startRead = System.nanoTime();
			ret = db.getResourceComments(keyname, keyname, Integer.parseInt(resourceID), cResult);
			if(ret < 0){
				System.out.println("There is an exception in getResourceComments.");
				System.exit(0);
			}
			long endRead = System.nanoTime();
			numOpsDone++;
			if(!warmup && enableLogging){
				readLog.append("READ,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startRead+","+endRead+","+cResult.size()+"\n");
				readsExist = true;
			}
		}	
		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doTransactionFriendProfile(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, int thinkTime,  boolean insertImage, boolean warmup)
	{	
		int numOpsDone=0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.getUserProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("FriendCount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("PendingCount")+"\n");
			readsExist = true;
		}

		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();
		ret = db.getTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		ret = db.getListOfFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getListOfFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		if(fResult.size() > 0){
			Random random = new Random();
			int idx = random.nextInt(fResult.size());
			HashMap<String,ByteIterator> fpResult=new HashMap<String,ByteIterator>();
			int friendId = -1;
			friendId = Integer.parseInt(fResult.get(idx).get("USERID").toString());
			startReadf = System.nanoTime();
			ret = db.getUserProfile(keyname, friendId, fpResult, insertImage, false);
			if(ret < 0){
				System.out.println("There is an exception in getProfile.");
				System.exit(0);
			}
			endReadf = System.nanoTime();
			numOpsDone++;
			if(!warmup && enableLogging){
				readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+friendId+","+startReadf+","+endReadf+","+fpResult.get("FriendCount")+"\n");
				if(keyname == friendId){
					readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+friendId+","+startReadf+","+endReadf+","+fpResult.get("PendingCount")+"\n");
				}
				readsExist = true;
			}
		}	

		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doTransactionPostCommentOnResource(DB db,int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, HashMap<String, Integer> resUpdateOperations, int thinkTime, boolean insertImage,  boolean warmup)
	{	
		int numOpsDone=0;
		int commentor = buildKeyName(usercount);
		commentor = activateUser(commentor);
		if(commentor == -1)
			return 0;
		incrUserRef(commentor);
		int keyname = buildKeyName(usercount);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.getUserProfile(commentor, commentor, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+commentor+","+startReadp+","+endReadp+","+pResult.get("FriendCount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+commentor+","+startReadp+","+endReadp+","+pResult.get("PendingCount")+"\n");
			readsExist = true;
		}

		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();
		ret = db.getTopKResources(commentor, commentor, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		pResult=new HashMap<String,ByteIterator>();
		startReadp = System.nanoTime();
		ret = db.getUserProfile(commentor, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("FriendCount")+"\n");
			if(keyname == commentor){
				readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("PendingCount")+"\n");
			}
			readsExist = true;
		}
		

		rResult=new Vector<HashMap<String,ByteIterator>>();
		ret = db.getTopKResources(commentor, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		if(rResult.size() > 0){
			Random random = new Random();
			int idx = random.nextInt(rResult.size());
			Vector<HashMap<String,ByteIterator>> cResult=new Vector<HashMap<String,ByteIterator>>();
			String resourceID = "";
			String ownerID ="";
			resourceID = rResult.get(idx).get("RID").toString();
			ownerID= rResult.get(idx).get("WALLUSERID").toString();
			cResult=new Vector<HashMap<String,ByteIterator>>();
			long startRead1 = System.nanoTime();
			ret = db.getResourceComments(keyname, keyname, Integer.parseInt(resourceID), cResult);
			
			if(ret < 0){
				System.out.println("There is an exception in getResourceComment.");
				System.exit(0);
			}
			long endRead1 = System.nanoTime();
			numOpsDone++;
			if(!warmup && enableLogging){
				readLog.append("READ,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startRead1+","+endRead1+","+cResult.size()+"\n");
				readsExist = true;
			}
			try {
				Thread.sleep(thinkTime);
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
			if(!warmup){
				long startUpdate = System.nanoTime();
				ret =db.postCommentOnResource(keyname, Integer.parseInt(ownerID), Integer.parseInt(resourceID));
				if(ret < 0){
					System.out.println("There is an exception in postComment.");
					System.exit(0);
				}
				long endUpdate = System.nanoTime();
				numOpsDone++;
				int numUpdatesTillNow = 0;

				if(resUpdateOperations.get(resourceID)!= null){
					numUpdatesTillNow = resUpdateOperations.get(resourceID);
				}
				resUpdateOperations.put(resourceID, (numUpdatesTillNow+1));
				if(enableLogging){
					updateLog.append("UPDATE,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startUpdate+","+endUpdate+","+(numUpdatesTillNow+1)+",I"+"\n");
					updatesExist = true;
				}
				try {
					Thread.sleep(thinkTime);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
			}
			long startRead2 = System.nanoTime();
			cResult=new Vector<HashMap<String,ByteIterator>>();
			ret = db.getResourceComments(keyname, keyname, Integer.parseInt(resourceID), cResult);
			if(ret < 0){
				System.out.println("There is an exception in getResourceComments.");
				System.exit(0);
			}
			long endRead2 = System.nanoTime();
			numOpsDone++;
			if(!warmup && enableLogging){
				readLog.append("READ,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startRead2+","+endRead2+","+cResult.size()+"\n");
				readsExist = true;
			}
		}	

		deactivateUser(commentor);
		return numOpsDone;
	}

	public int doTransactionGenerateFriendship(DB db, int threadid, StringBuilder updateLog, StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime,  boolean insertImage, boolean warmup)
	{
		int numOpsDone=0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.getUserProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("FriendCount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("PendingCount")+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		ret = db.getTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		ret = db.getListOfFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getListOfFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		int noRelId = -1; 
		noRelId = viewNotRelatedUsers(keyname);
		//if A invited B, B should not be able to invite A else with reject and invite again you will have integrity constraints
		if(noRelId!= -1 && isActive(noRelId) == -1){
			deactivateUser(keyname);
			return numOpsDone;
		}
		if(!warmup){
			if(noRelId == -1){
				//do nothing
			}else{
				long startUpdatei = System.nanoTime();
				ret = db.inviteFriends(keyname, noRelId);
				if(ret < 0){
					System.out.println("There is an exception in invFriends.");
					System.exit(0);
				}
				int numPendingsForOtherUserTillNow = 0;
				if(pendingInfo.get(Integer.toString(noRelId))!= null){
					numPendingsForOtherUserTillNow = pendingInfo.get(Integer.toString(noRelId));
				}
				pendingInfo.put(Integer.toString(noRelId), (numPendingsForOtherUserTillNow+1));
				long endUpdatei = System.nanoTime();
				numOpsDone++;
				if(enableLogging){
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+noRelId+","+startUpdatei+","+endUpdatei+","+(numPendingsForOtherUserTillNow+1)+",I"+"\n");
					updatesExist = true;
				}
				relateUsers(keyname, noRelId );
				deactivateUser(noRelId);
				try {
					Thread.sleep(thinkTime);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
			}	
		}

		Vector<HashMap<String,ByteIterator>> peResult=new Vector<HashMap<String,ByteIterator>>();
		startReadp = System.nanoTime();
		ret = db.viewPendingRequests(keyname, peResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in viewPendingFriends.");
			System.exit(0);
		}
		endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doTransactionAcceptFriendship(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime,  boolean insertImage, boolean warmup)
	{		
		int numOpsDone =0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.getUserProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("FriendCount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("PendingCount")+"\n");
			readsExist = true;
		}
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		ret = db.getTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		ret = db.getListOfFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getListOfFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		Vector<HashMap<String,ByteIterator>> peResult=new Vector<HashMap<String,ByteIterator>>();
		startReadp = System.nanoTime();
		ret = db.viewPendingRequests(keyname, peResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in viewPendingRequests.");
			System.exit(0);
		}
		endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		if(!warmup){
			if(peResult.size() == 0){
				//do nothing
			}else{
				Random random = new Random();
				int idx = random.nextInt(peResult.size());
				long startUpdatea = System.nanoTime();
				String auserid = "";
				auserid =peResult.get(idx).get("USERID").toString();
				ret = db.acceptFriendRequest(Integer.parseInt(auserid), keyname);
				if(ret < 0){
					System.out.println("There is an exception in acceptFriendRequests.");
					System.exit(0);
				}
				int numFriendsForThisUserTillNow = 0;
				if(friendshipInfo.get(Integer.toString(keyname))!= null){
					numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(keyname));
				}
				friendshipInfo.put(Integer.toString(keyname), (numFriendsForThisUserTillNow+1));

				int numFriendsForOtherUserTillNow = 0;
				if(friendshipInfo.get(auserid)!= null){
					numFriendsForOtherUserTillNow = friendshipInfo.get(auserid);
				}
				friendshipInfo.put(auserid, (numFriendsForOtherUserTillNow+1));
				int numPendingsForThisUserTillNow = 0;
				if(pendingInfo.get(Integer.toString(keyname))!= null){
					numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(keyname));
				}
				pendingInfo.put(Integer.toString(keyname), (numPendingsForThisUserTillNow-1));
				long endUpdatea = System.nanoTime();
				numOpsDone++;
				if(enableLogging){
					updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+auserid+","+startUpdatea+","+endUpdatea+","+(numFriendsForOtherUserTillNow+1)+",I"+"\n");
					updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numFriendsForThisUserTillNow+1)+",I"+"\n");
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+"\n");
					updatesExist = true;
				}
				relateUsers(keyname, Integer.parseInt(auserid));
				try {
					Thread.sleep(thinkTime);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
			}
		}

		fResult=new Vector<HashMap<String,ByteIterator>>();
		startReadf = System.nanoTime();
		ret = db.getListOfFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getListOfFriends.");
			System.exit(0);
		}
		endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		peResult=new Vector<HashMap<String,ByteIterator>>();
		startReadp = System.nanoTime();
		ret = db.viewPendingRequests(keyname, peResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in viewPendingRequests.");
			System.exit(0);
		}
		endReadp = System.nanoTime();
		numOpsDone++;

		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
			readsExist = true;
		}
		deactivateUser(keyname);
		return numOpsDone;

	}

	public int doTransactionRejectFriendship(DB db, int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime,  boolean insertImage, boolean warmup)
	{		
		int numOpsDone =0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.getUserProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("FriendCount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("PendingCount")+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		ret = db.getTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		ret = db.getListOfFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getListOfFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> peResult=new Vector<HashMap<String,ByteIterator>>();
		startReadp = System.nanoTime();
		ret = db.viewPendingRequests(keyname, peResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in viewPendingRequests.");
			System.exit(0);
		}
		endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		if(!warmup){
			if(peResult.size() == 0){
				//do nothing
			}else{
				Random random = new Random();
				int idx = random.nextInt(peResult.size());
				long startUpdatea = System.nanoTime();
				String auserid = "";
				auserid =peResult.get(idx).get("USERID").toString();
				ret = db.rejectFriendRequest(Integer.parseInt(auserid), keyname);
				if(ret < 0){
					System.out.println("There is an exception in rejectFriendRequest.");
					System.exit(0);
				}
				int numPendingsForThisUserTillNow = 0;
				if(pendingInfo.get(Integer.toString(keyname))!= null){
					numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(keyname));
				}
				pendingInfo.put(Integer.toString(keyname), (numPendingsForThisUserTillNow-1));
				long endUpdatea = System.nanoTime();
				numOpsDone++;
				if(enableLogging){
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+"\n");
					updatesExist = true;
				}
				deRelateUsers(keyname, Integer.parseInt(auserid) );
				try {
					Thread.sleep(thinkTime);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
			}
		}

		fResult=new Vector<HashMap<String,ByteIterator>>();
		startReadf = System.nanoTime();
		ret = db.getListOfFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getListOfFriends.");
			System.exit(0);
		}
		endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		peResult=new Vector<HashMap<String,ByteIterator>>();
		startReadp = System.nanoTime();
		ret = db.viewPendingRequests(keyname, peResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in viewPendingRequests.");
			System.exit(0);
		}
		endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
			readsExist = true;
		}
		deactivateUser(keyname);
		return numOpsDone;

	}

	public int doTransactionUnfriendFriendship(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime,   boolean insertImage, boolean warmup)
	{	
		int numOpsDone =0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.getUserProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("FriendCount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("PendingCount")+"\n");
			readsExist = true;
		}
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		ret = db.getTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResources.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		ret = db.getListOfFriends(keyname, keyname, null, fResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getListOfFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		if(!warmup){
			if(fResult.size() == 0){
				//do nothing
			}else{
				Random random = new Random();
				int idx = random.nextInt(fResult.size());
				long startUpdater = System.nanoTime();
				String auserid = "";
				auserid =fResult.get(idx).get("USERID").toString();

				if(isActive(Integer.parseInt(auserid)) != -1){			

					ret = db.unFriendFriend(Integer.parseInt(auserid), keyname);
					if(ret < 0){
						System.out.println("There is an exception in unFriendFriend.");
						System.exit(0);
					}
					int numFriendsForThisUserTillNow = 0;
					if(friendshipInfo.get(Integer.toString(keyname))!= null){
						numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(keyname));
					}
					friendshipInfo.put(Integer.toString(keyname), (numFriendsForThisUserTillNow-1));

					int numFriendsForOtherUserTillNow = 0;
					if(friendshipInfo.get(auserid)!= null){
						numFriendsForOtherUserTillNow = friendshipInfo.get(auserid);
					}
					friendshipInfo.put(auserid, (numFriendsForOtherUserTillNow-1));
					long endUpdater = System.nanoTime();
					numOpsDone++;

					if(enableLogging){
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+auserid+","+startUpdater+","+endUpdater+","+(numFriendsForOtherUserTillNow-1)+",D"+"\n");
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startUpdater+","+endUpdater+","+(numFriendsForThisUserTillNow-1)+",D"+"\n");
						updatesExist = true;
					}
					deRelateUsers(keyname, Integer.parseInt(auserid));
					deactivateUser(Integer.parseInt(auserid));

					try {
						Thread.sleep(thinkTime);
					} catch (InterruptedException e) {
						e.printStackTrace(System.out);
					}
				}
			}
		}
		fResult=new Vector<HashMap<String,ByteIterator>>();
		startReadf = System.nanoTime();
		ret = db.getListOfFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getListOfFriends.");
			System.exit(0);
		}
		endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		deactivateUser(keyname);
		return numOpsDone;

	}


	public int doTransactionUnfriendPendingFriendship(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime, String nextOp,  boolean insertImage, boolean warmup)
	{	
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.getUserProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("FriendCount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("PendingCount")+"\n");
			readsExist = true;
		}
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		ret = db.getTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		if(!warmup){
			int noRelId = -1;
			noRelId = viewNotRelatedUsers(keyname);
			if(noRelId == -1){
				//do nothing
			}else{
				if(isActive(noRelId) != -1){
					long startUpdatei = System.nanoTime();
					ret = db.inviteFriends(keyname, noRelId);	
					if(ret < 0){
						System.out.println("There is an exception in inviteFriends.");
						System.exit(0);
					}
					int numPendingsForOtherUserTillNow = 0;
					if(pendingInfo.get(Integer.toString(noRelId))!= null){
						numPendingsForOtherUserTillNow = pendingInfo.get(Integer.toString(noRelId));
					}
					pendingInfo.put(Integer.toString(noRelId), (numPendingsForOtherUserTillNow+1));
					long endUpdatei = System.nanoTime();
					numOpsDone++;
					if(enableLogging){
						updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+noRelId+","+startUpdatei+","+endUpdatei+","+(numPendingsForOtherUserTillNow+1)+",I"+"\n");
						updatesExist = true;
					}
					relateUsers(keyname, noRelId);
					deactivateUser(noRelId);
					try {
						Thread.sleep(thinkTime);
					} catch (InterruptedException e) {
						e.printStackTrace(System.out);
					}
				}
			}
		}
		Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		ret = db.getListOfFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getListOfFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist= true;
		}
		if(!warmup){
			if(fResult.size() == 0){
				//do nothing
			}else{
				Random random = new Random();
				int idx = random.nextInt(fResult.size());
				long startUpdater = System.nanoTime();
				String auserid = "";
				auserid =fResult.get(idx).get("USERID").toString();

				if(isActive(Integer.parseInt(auserid)) != -1){			

					ret = db.unFriendFriend(Integer.parseInt(auserid), keyname);
					if(ret < 0){
						System.out.println("There is an exception in unFriendFriend.");
						System.exit(0);
					}
					int numFriendsForThisUserTillNow = 0;
					if(friendshipInfo.get(Integer.toString(keyname))!= null){
						numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(keyname));
					}
					friendshipInfo.put(Integer.toString(keyname), (numFriendsForThisUserTillNow-1));


					int numFriendsForOtherUserTillNow = 0;
					if(friendshipInfo.get(auserid)!= null){
						numFriendsForOtherUserTillNow = friendshipInfo.get(auserid);
					}
					friendshipInfo.put(auserid, (numFriendsForOtherUserTillNow-1));
					long endUpdater = System.nanoTime();
					numOpsDone++;
					if(enableLogging){
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+auserid+","+startUpdater+","+endUpdater+","+(numFriendsForOtherUserTillNow-1)+",D"+"\n");
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startUpdater+","+endUpdater+","+(numFriendsForThisUserTillNow-1)+",D"+"\n");
						updatesExist = true;
					}
					deRelateUsers(keyname, Integer.parseInt(auserid));
					deactivateUser(Integer.parseInt(auserid));
					try {
						Thread.sleep(thinkTime);
					} catch (InterruptedException e) {
						e.printStackTrace(System.out);
					}
				}
			}
		}
		fResult=new Vector<HashMap<String,ByteIterator>>();
		startReadf = System.nanoTime();
		ret = db.getListOfFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getListOfFriends.");
			System.exit(0);
		}
		endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		Vector<HashMap<String,ByteIterator>> peResult=new Vector<HashMap<String,ByteIterator>>();
		startReadp = System.nanoTime();
		ret = db.viewPendingRequests(keyname, peResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in viewPendingRequests.");
			System.exit(0);
		}
		endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		if(!warmup){
			if(peResult.size() == 0){
				//do nothing
			}else{
				Random random = new Random();
				int idx = random.nextInt(peResult.size());
				long startUpdatea = System.nanoTime();
				if(nextOp.equals("ACCEPT")){
					String auserid = "";
					auserid =peResult.get(idx).get("USERID").toString();
					ret = db.acceptFriendRequest(Integer.parseInt(auserid), keyname);
					if(ret < 0){
						System.out.println("There is an exception in acceptFriendRequests.");
						System.exit(0);
					}
					int numFriendsForThisUserTillNow = 0;
					if(friendshipInfo.get(Integer.toString(keyname))!= null){
						numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(keyname));
					}
					friendshipInfo.put(Integer.toString(keyname), (numFriendsForThisUserTillNow+1));


					int numFriendsForOtherUserTillNow = 0;
					if(friendshipInfo.get(auserid)!= null){
						numFriendsForOtherUserTillNow = friendshipInfo.get(auserid);
					}
					friendshipInfo.put(auserid, (numFriendsForOtherUserTillNow+1));
					int numPendingsForThisUserTillNow = 0;
					if(pendingInfo.get(Integer.toString(keyname))!= null){
						numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(keyname));
					}
					pendingInfo.put(Integer.toString(keyname), (numPendingsForThisUserTillNow-1));
					long endUpdatea = System.nanoTime();
					numOpsDone++;
					if(enableLogging){
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+auserid+","+startUpdatea+","+endUpdatea+","+(numFriendsForOtherUserTillNow+1)+",I"+"\n");
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numFriendsForThisUserTillNow+1)+",I"+"\n");
						updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+"\n");
						updatesExist = true;
					}
					relateUsers(keyname, Integer.parseInt(auserid));
				}else if(nextOp.equals("REJECT")){
					String auserid = "";
					auserid =peResult.get(idx).get("USERID").toString();
					ret = db.rejectFriendRequest(Integer.parseInt(auserid), keyname);
					if(ret < 0){
						System.out.println("There is an exception in rejectFriendRequest.");
						System.exit(0);
					}
					int numPendingsForThisUserTillNow = 0;
					if(pendingInfo.get(Integer.toString(keyname))!= null){
						numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(keyname));
					}
					pendingInfo.put(Integer.toString(keyname), (numPendingsForThisUserTillNow-1));
					long endUpdatea = System.nanoTime();
					numOpsDone++;
					if(enableLogging){
						updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+"\n");
						updatesExist = true;
					}
					deRelateUsers(keyname, Integer.parseInt(auserid) );
				}
				try {
					Thread.sleep(thinkTime);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
			}
		}

		fResult=new Vector<HashMap<String,ByteIterator>>();
		startReadf = System.nanoTime();
		ret = db.getListOfFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getListOfFriends.");
			System.exit(0);
		}
		endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		peResult=new Vector<HashMap<String,ByteIterator>>();
		startReadp = System.nanoTime();
		ret = db.viewPendingRequests(keyname, peResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in viewPendingRequests.");
			System.exit(0);
		}
		endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
			readsExist = true;
		}

		deactivateUser(keyname);
		return numOpsDone;
	}


	public int doActionGetProfile(DB db, int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, boolean insertImage,  boolean warmup)
	{		
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
		long startReadp = System.nanoTime();
		int ret = db.getUserProfile(keyname, keyname, pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("FriendCount")+"\n");
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+pResult.get("PendingCount")+"\n");
			readsExist = true;
		}
		deactivateUser(keyname);
		return numOpsDone;
	}


	public int doActionGetFriends(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		int ret = db.getListOfFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getListOfFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}

		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doActionGetPendings(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		Vector<HashMap<String,ByteIterator>> pResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		int ret = db.viewPendingRequests(keyname,pResult, insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in viewPendingRequests.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+pResult.size()+"\n");
			readsExist = true;
		}

		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doActionInviteFriends(DB db, int threadid, StringBuilder updateLog, StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);

		int noRelId = -1;
		noRelId = viewNotRelatedUsers(keyname);
		if(noRelId!= -1 && isActive(noRelId) == -1){
			deactivateUser(keyname);
			return numOpsDone;
		}
		if(!warmup){
			if(noRelId == -1){
				//do nothing
			}else{
				long startUpdatei = System.nanoTime();
				int ret = db.inviteFriends(keyname, noRelId);
				pendingFrnds.get(noRelId).add(keyname);
				if(ret < 0){
					System.out.println("There is an exception in inviteFriends.");
					System.exit(0);
				}
				int numPendingsForOtherUserTillNow = 0;
				if(pendingInfo.get(Integer.toString(noRelId))!= null){
					numPendingsForOtherUserTillNow = pendingInfo.get(Integer.toString(noRelId));
				}
				pendingInfo.put(Integer.toString(noRelId), (numPendingsForOtherUserTillNow+1));
				long endUpdatei = System.nanoTime();
				numOpsDone++;
				if(enableLogging){
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+noRelId+","+startUpdatei+","+endUpdatei+","+(numPendingsForOtherUserTillNow+1)+",I"+"\n");
					updatesExist= true;
				}
				relateUsers(keyname, noRelId );
				deactivateUser(noRelId);
			}	
		}

		deactivateUser(keyname);
		return numOpsDone;
	}


	public int doActionAcceptFriends(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime,  boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname); 
		/*Vector<HashMap<String,ByteIterator>> peResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadp = System.nanoTime();
		int ret = db.viewPendingRequests(keyname, peResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in viewPendingRequests.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}*/
		if(!warmup){
			/*if(peResult.size() == 0){
				//do nothing
			}else*/{
				int auserid = -1;
				//Random random = new Random();
				//int idx = random.nextInt(peResult.size());
				//auserid =Integer.parseInt(peResult.get(idx).get("USERID").toString());
				//needed for only accept friendship, change all presult to ids
				Vector<Integer> ids =pendingFrnds.get(keyname);
		if(ids.size() > 0){
				auserid = ids.get(ids.size()-1);
				//remove from the list coz it has been accepted
				ids.remove(ids.size()-1);
				long startUpdatea = System.nanoTime();
				int ret = 0;
				ret = db.acceptFriendRequest(auserid, keyname);
				try {
					aFrnds.acquire();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				acceptedFrnds.get(auserid).put(keyname,"");
				acceptedFrnds.get(keyname).put(auserid,"");	
				aFrnds.release();
				
				if(ret < 0){
					System.out.println("There is an exception in acceptFriendRequest.");
					System.exit(0);
				}
				int numFriendsForThisUserTillNow = 0;
				if(friendshipInfo.get(Integer.toString(keyname))!= null){
					numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(keyname));
				}
				friendshipInfo.put(Integer.toString(keyname), (numFriendsForThisUserTillNow+1));


				int numFriendsForOtherUserTillNow = 0;
				if(friendshipInfo.get(auserid)!= null){
					numFriendsForOtherUserTillNow = friendshipInfo.get(auserid);
				}
				friendshipInfo.put(Integer.toString(auserid), (numFriendsForOtherUserTillNow+1));
				int numPendingsForThisUserTillNow = 0;
				if(pendingInfo.get(Integer.toString(keyname))!= null){
					numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(keyname));
				}
				pendingInfo.put(Integer.toString(keyname), (numPendingsForThisUserTillNow-1));
				long endUpdatea = System.nanoTime();
				numOpsDone++;
				if(enableLogging){
					updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+auserid+","+startUpdatea+","+endUpdatea+","+(numFriendsForOtherUserTillNow+1)+",I"+"\n");
					updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numFriendsForThisUserTillNow+1)+",I"+"\n");
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+"\n");
					updatesExist = true;
				}
				relateUsers(keyname,auserid);
				}
			}
		}
		deactivateUser(keyname);
		return numOpsDone;

	}

	public int doActionRejectFriends(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime,  boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		/*Vector<HashMap<String,ByteIterator>> peResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadp = System.nanoTime();
		int ret = db.viewPendingRequests(keyname, peResult,  insertImage, false);
		if(ret == -1){
			System.out.println("There is an exception in viewPendingRequests.");
			System.exit(0);
		}
		long endReadp = System.nanoTime();
		numOpsDone++;
		if(!warmup && enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+threadid+","+keyname+","+startReadp+","+endReadp+","+peResult.size()+"\n");
			readsExist = true;
		}
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}*/
		if(!warmup){
			/*if(peResult.size() == 0){
				// do nothing
			}else*/{
		int auserid = -1;
		Vector<Integer> ids =pendingFrnds.get(keyname);
		if(ids.size() > 0){
				auserid = ids.get(ids.size()-1);
				//remove from the list coz it has been accepted
				ids.remove(ids.size()-1);
				int ret = 0;			
				/*Random random = new Random();
				int idx = random.nextInt(peResult.size());
				auserid =Integer.parseInt(peResult.get(idx).get("USERID").toString());*/
				long startUpdatea = System.nanoTime();
				ret = db.rejectFriendRequest(auserid, keyname);
				if(ret < 0){
					System.out.println("There is an exception in rejectFriendRequest.");
					System.exit(0);
				}
				int numPendingsForThisUserTillNow = 0;
				if(pendingInfo.get(Integer.toString(keyname))!= null){
					numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(keyname));
				}
				pendingInfo.put(Integer.toString(keyname), (numPendingsForThisUserTillNow-1));
				long endUpdatea = System.nanoTime();
				numOpsDone++;
				if(enableLogging){
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+keyname+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+"\n");
					updatesExist = true;
				}
				deRelateUsers(keyname, auserid );
				}
			}
		}
		deactivateUser(keyname);
		return numOpsDone;

	}

	public int doActionUnFriendFriends(DB db, int threadid, StringBuilder updateLog,StringBuilder readLog, int seqID, HashMap<String, Integer> friendshipInfo
			,HashMap<String, Integer> pendingInfo, int thinkTime,   boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		/*Vector<HashMap<String,ByteIterator>> fResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		int ret = db.getListOfFriends(keyname, keyname, null, fResult,  insertImage, false);
		if(ret < 0){
			System.out.println("There is an exception in getListOfFriends.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		if(!warmup && enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startReadf+","+endReadf+","+fResult.size()+"\n");
			readsExist = true;
		}*/
		if(!warmup){
			/*if(fResult.size() == 0){
				// do nothing
			}else{*/
			int ret = 0;
			if(acceptedFrnds.get(keyname).size() > 0 ){	
				int idx = 0;
				//Random random = new Random();
				//int idx = random.nextInt(acceptedFrnds.get(keyname).size());
				//auserid =Integer.parseInt(fResult.get(idx).get("USERID").toString());
				//no need for locks and activation coz they cant invite again until they have been derelated
				//and its ok if two users delete each other at the same time
				//TODO: can still add a lock just for delete tho
				long startUpdater = System.nanoTime();
				int auserid = -1;
				try {
					aFrnds.acquire();
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
				Set<Integer> keys = acceptedFrnds.get(keyname).keySet();
				Iterator<Integer> it = keys.iterator();
				auserid = it.next();	
				if(isActive(auserid) != -1){//no needed	
					//System.out.println(auserid+" "+keyname);
					ret = db.unFriendFriend(auserid, keyname);
					if(ret < 0){
						System.out.println("There is an exception in unFriendFriend.");
						System.exit(0);
					}
					//remove from acceptedFrnds
					acceptedFrnds.get(keyname).remove(auserid);
					acceptedFrnds.get(auserid).remove(keyname);
					
					int numFriendsForThisUserTillNow = 0;
					if(friendshipInfo.get(Integer.toString(keyname))!= null){
						numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(keyname));
					}
					friendshipInfo.put(Integer.toString(keyname), (numFriendsForThisUserTillNow-1));


					int numFriendsForOtherUserTillNow = 0;
					if(friendshipInfo.get(auserid)!= null){
						numFriendsForOtherUserTillNow = friendshipInfo.get(auserid);
					}
					friendshipInfo.put(Integer.toString(auserid), (numFriendsForOtherUserTillNow-1));
					long endUpdater = System.nanoTime();
					numOpsDone++;
					if(enableLogging){
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+auserid+","+startUpdater+","+endUpdater+","+(numFriendsForOtherUserTillNow-1)+",D"+"\n");
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadid+","+keyname+","+startUpdater+","+endUpdater+","+(numFriendsForThisUserTillNow-1)+",D"+"\n");
						updatesExist = true;
					}
					deRelateUsers(keyname, auserid);
					deactivateUser(auserid);
				}
				aFrnds.release();
			}
		}
		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doActionGetTopResources(DB db, int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		int ret = db.getTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		deactivateUser(keyname);
		return numOpsDone;
	}

	public int doActionGetResourceComments(DB db, int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, int thinkTime, boolean insertImage,  boolean warmup)
	{
		int numOpsDone = 0;
		int keyname = buildKeyName(usercount);
		keyname = activateUser(keyname);
		if(keyname == -1)
			return 0;
		incrUserRef(keyname);
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();		
		int ret = db.getTopKResources(keyname, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}

		if(rResult.size() > 0){
			Random random = new Random();
			int idx = random.nextInt(rResult.size());
			Vector<HashMap<String,ByteIterator>> cResult=new Vector<HashMap<String,ByteIterator>>();
			String resourceID ="";
			resourceID = rResult.get(idx).get("RID").toString();
			long startRead = System.nanoTime();
			ret = db.getResourceComments(keyname, keyname, Integer.parseInt(resourceID), cResult);
			if(ret < 0){
				System.out.println("There is an exception in getResourceComment.");
				System.exit(0);
			}
			long endRead = System.nanoTime();
			numOpsDone++;
			if(!warmup && enableLogging){
				readLog.append("READ,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startRead+","+endRead+","+cResult.size()+"\n");
				readsExist = true;
			}			
		}	
		deactivateUser(keyname);
		return numOpsDone;

	}

	public int doActionPostComments(DB db,int threadid, StringBuilder updateLog, StringBuilder readLog,int seqID, HashMap<String, Integer> resUpdateOperations, int thinkTime, boolean insertImage, boolean warmup)
	{
		int numOpsDone = 0;
		int commentor = buildKeyName(usercount);
		commentor = activateUser(commentor);
		if(commentor == -1)
			return 0;
		incrUserRef(commentor);
		int keyname = buildKeyName(usercount);
		Vector<HashMap<String,ByteIterator>> rResult=new Vector<HashMap<String,ByteIterator>>();
		int ret = db.getTopKResources(commentor, keyname, 5, rResult);
		if(ret < 0){
			System.out.println("There is an exception in getTopResource.");
			System.exit(0);
		}
		numOpsDone++;
		try {
			Thread.sleep(thinkTime);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		if(rResult.size() > 0){
			Random random = new Random();
			int idx = random.nextInt(rResult.size());
			Vector<HashMap<String,ByteIterator>> cResult=new Vector<HashMap<String,ByteIterator>>();
			String resourceID = "";
			String ownerID ="";
			resourceID = rResult.get(idx).get("RID").toString();
			ownerID= rResult.get(idx).get("WALLUSERID").toString();
			if(!warmup){
				long startUpdate = System.nanoTime();
				ret =db.postCommentOnResource(keyname, Integer.parseInt(ownerID), Integer.parseInt(resourceID)); 
				if(ret < 0){
					System.out.println("There is an exception in postComment.");
					System.exit(0);
				}
				long endUpdate = System.nanoTime();
				numOpsDone++;
				int numUpdatesTillNow = 0;

				if(resUpdateOperations.get(resourceID)!= null){
					numUpdatesTillNow = resUpdateOperations.get(resourceID);
				}
				resUpdateOperations.put(resourceID, (numUpdatesTillNow+1));
				if(enableLogging){
					updateLog.append("UPDATE,POSTCOMMENT,"+seqID+","+threadid+","+resourceID+","+startUpdate+","+endUpdate+","+(numUpdatesTillNow+1)+",I"+"\n");
					updatesExist = true;
				}
			}
		}	
		deactivateUser(commentor);
		return numOpsDone;

	}


	@Override
	public boolean doInsert(DB db, Object threadstate) {
		return false;
	}


	public boolean isRelated(int uid1, int uid2){
		boolean related = false;
		try {
			rStat.acquire();
			if(userRelations.get(uid1) != null){
				HashMap<Integer, String> rels = userRelations.get(uid1);
				if(rels.containsKey(uid2)){
					related= true;
				}else
					related = false;
			}else
				related = false;
			if(userRelations.get(uid2) != null){
				HashMap<Integer, String> rels = userRelations.get(uid2);
				if(rels.containsKey(uid1)){
					related = true;
				}else
					related = false;
			}else
				related = false;
			rStat.release();
		} catch (Exception e) {
			System.out.println("Error in Rels");
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		return related;		
	}


	public void relateUsers(int uid1, int uid2){
		try {

			rStat.acquire();
			if(userRelations.get(uid1) != null){
				HashMap<Integer, String> rels = userRelations.get(uid1);
				if(!rels.containsKey(uid2)){
					rels.put(uid2,"");
				}
				userRelations.put(uid1, rels);
			}else{
				HashMap<Integer, String> rels = new HashMap<Integer, String>();
				rels.put(uid2,"");
				userRelations.put(uid1, rels);
			}

			if(userRelations.get(uid2) != null){
				HashMap<Integer, String> rels = userRelations.get(uid2);
				if(!rels.containsKey(uid1)){
					rels.put(uid1,"");
				}
				userRelations.put(uid2, rels);
			}else{
				HashMap<Integer, String> rels = new HashMap<Integer, String>();
				rels.put(uid1,"");
				userRelations.put(uid2, rels);	
			}

			rStat.release();
		} catch (Exception e) {
			System.out.println("Error in Rels");
			e.printStackTrace(System.out);
			System.exit(-1);
		}		
	}

	public void deRelateUsers(int uid1, int uid2){
		try {
			rStat.acquire();
			HashMap<Integer, String> rels = userRelations.get(uid1);
			if(rels.containsKey(uid2)){
				rels.remove(uid2);
			}
			userRelations.put(uid1, rels);

			rels = userRelations.get(uid2);
			if(rels.containsKey(uid1)){
				rels.remove(uid1);
			}
			userRelations.put(uid2, rels);
			rStat.release();
		} catch (Exception e) {
			System.out.println("Error in Rels");
			e.printStackTrace(System.out);
			System.exit(-1);
		}	
	}

	public int viewNotRelatedUsers(int uid){
		int key = -1;
		try{
			rStat.acquire();
			HashMap<Integer, String> rels = userRelations.get(uid);
			int id = buildKeyName(usercount) ;
			int idx = memberIdxs.get(id);
			//int idx = random.nextInt(usercount)+useroffset;
			for(int i=idx; i<idx+usercount; i++){
				if(!rels.containsKey(myMembers[i%usercount])){
					key = myMembers[i%usercount];
					break;
				}
			}
			if(key == -1)
				System.out.println("No more friends to allocate for  "+uid+" ; benchmark results invalid");
			rStat.release();	
		}catch(Exception e){
			System.out.println("Error in view not related");
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		return key;
	}

	public int isActive(int uid){
		//if active return -1 
				//else return 0
				int actualIdx = memberIdxs.get(uid);
				int shardIdx = myMemberObjs[actualIdx].get_shardIdx();
				int idxInShard = myMemberObjs[actualIdx].get_idxInShard();
				
				try {
					uStatSemaphores[shardIdx].acquire();
					if (userStatusShards[shardIdx][idxInShard] == 'a'){
						//user is active
						uStatSemaphores[shardIdx].release();
						return -1;
					}else{
						//user is not active
						//activate it
						userStatusShards[shardIdx][idxInShard] = 'a';
						uStatSemaphores[shardIdx].release();
						return 0;
					}	
				} catch (Exception e) {
					System.out.println("Error-Cant activate any user");
					e.printStackTrace(System.out);
					System.exit(-1);
				}
				return -1;
				}

	public int activateUser(int uid)
	{
		try {
			int actualIdx = memberIdxs.get(uid);
			int shardIdx = myMemberObjs[actualIdx].get_shardIdx();
			int idxInShard = myMemberObjs[actualIdx].get_idxInShard();
			uStatSemaphores[shardIdx].acquire();
			int cnt =0; //needed for avoiding loops
			//int shardscnt = 0;
			//find a free member within this shard
			while (userStatusShards[shardIdx][idxInShard] != 'd'){
				if(cnt == userStatusShards[shardIdx].length){
					uStatSemaphores[shardIdx].release();
					/*shardscnt ++;
					if(shardscnt == numShards){ //went through all the shards once
						return -1;
					}
					shardIdx = (shardIdx+1)%numShards;
					cnt = 0;
					idxInShard = 0;
					actualIdx = numShards*idxInShard+shardIdx;
					uid = myMemberObjs[actualIdx].get_uid();
					uStatSemaphores[shardIdx].acquire();
					continue;*/
					return -1;
				}
				idxInShard = (idxInShard+1);
				idxInShard = idxInShard%userStatusShards[shardIdx].length;
				//map to actual idx
				actualIdx = numShards*idxInShard+shardIdx;
				uid = myMemberObjs[actualIdx].get_uid();
				cnt++;
			}
			userStatusShards[shardIdx][idxInShard] ='a';
			uStatSemaphores[shardIdx].release();
		} catch (Exception e) {
			System.out.println("Error-Cant activate any user");
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		return uid;
	}


	public void deactivateUser(int uid)
	{
		int actualIdx = memberIdxs.get(uid);
		int shardIdx = myMemberObjs[actualIdx].get_shardIdx();
		int idxInShard = myMemberObjs[actualIdx].get_idxInShard();
		
		try {
			uStatSemaphores[shardIdx].acquire();
			if (userStatusShards[shardIdx][idxInShard] == 'd') {
				System.out.println("Error - The user is already deactivated");	
			}
			userStatusShards[shardIdx][idxInShard]='d'; //Mark as available
			uStatSemaphores[shardIdx].release();
		} catch (Exception e) {
			System.out.println("Error - couldnt deactivate user");
			e.printStackTrace(System.out);
			System.exit(-1);
		}

		return ;
	}

	public void incrUserRef(int uid)
	{
		int actualIdx = memberIdxs.get(uid);
		int shardIdx = myMemberObjs[actualIdx].get_shardIdx();
		int idxInShard = myMemberObjs[actualIdx].get_idxInShard();		
		try {
			uFreqSemaphores[shardIdx].acquire();
			userFreqShards[shardIdx][idxInShard]=userFreqShards[shardIdx][idxInShard]+1;  
			uFreqSemaphores[shardIdx].release();
		} catch (Exception e) {
			System.out.println("Error-Cant increament users frequency of access");
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		return;
	}



	@Override
	public HashMap<String, String> getDBInitialStats(DB db) {
		HashMap<String, String> stats = new HashMap<String, String>();
		stats = db.getInitialStats();
		return stats;
	}

	public static String getFrequecyStats(){
		String userFreqStats = "";
		//int sum = 0;
		for(int i=0; i<myMemberObjs.length; i++){
			userFreqStats+=myMemberObjs[i].get_uid()+" ,"+userFreqShards[myMemberObjs[i].get_shardIdx()][myMemberObjs[i].get_idxInShard()]+"\n";
			//sum += userFreqShards[myMemberObjs[i].get_shardIdx()][myMemberObjs[i].get_idxInShard()];	
		}
				
		return userFreqStats;
	}

}
