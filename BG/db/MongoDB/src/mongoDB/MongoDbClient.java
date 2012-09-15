/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by sumita barahmand
 *
 *
 *
 */

package mongoDB;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.gridfs.*;

import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

/**
 * MongoDB client for BG framework.
 *
 * Properties to set:
 *
 * mongodb.url=mongodb://localhost:27017
 * mongodb.database=benchmark
 * mongodb.writeConcern=normal
 *
 * 
 *
 */
public class MongoDbClient extends DB implements MongoDBClientConstants {

	private Mongo mongo;
	private WriteConcern writeConcern;
	private String database;
	private boolean manipulationArray;
	private boolean friendListReq;
	private static String imagepath = "";
	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	public void init() throws DBException {
		// initialize MongoDb driver
		Properties props = getProperties();
		String url = props.getProperty(MONGODB_URL_PROPERTY);
		database = props.getProperty(MONGODB_DB_PROPERTY);
		String writeConcernType = props.getProperty(MONGODB_WRITE_CONCERN_PROPERTY);
		manipulationArray = Boolean.parseBoolean(props.getProperty(MONGODB_MANIPULATION_ARRAY_PROPERTY, MONGODB_MANIPULATION_ARRAY_PROPERTY_DEFAULT));
		friendListReq = Boolean.parseBoolean(props.getProperty(MONGODB_FRNDLIST_REQ_PROPERTY, MONGODB_FRNDLIST_REQ_PROPERTY_DEFAULT));
		imagepath = props.getProperty(Client.IMAGE_PATH_PROPERTY, Client.IMAGE_PATH_PROPERTY_DEFAULT);
		
		if ("none".equals(writeConcernType)) {
			//don't return error on writes
			writeConcern = WriteConcern.NONE;
		} else if ("strict".equals(writeConcernType)) {
			//normal error handling - just raise exceptions when problems, don wait for response form servers
			writeConcern = WriteConcern.SAFE;
		} else if ("normal".equals(writeConcernType)) {
			//The write will wait for a response from the server and raise an exception on any error
			writeConcern = WriteConcern.NORMAL;
		}

		try {
			// strip out prefix since Java driver doesn't currently support
			// standard connection format URL yet
			// http://www.mongodb.org/display/DOCS/Connections
			if (url.startsWith("mongodb://")) {
				url = url.substring(10);
			}

			// need to append db to url.
			url += "/"+database;
			//System.out.println("new database url = "+url);
			/*MongoOptions mo = new MongoOptions();
			mo.connectionsPerHost = 100;
			mongo = new Mongo(new DBAddress(url), mo);*/
			mongo = new Mongo(new DBAddress(url));
			//System.out.println("mongo connection created with "+url);
		} catch (Exception e1) {
			System.out.println(
					"Could not initialize MongoDB connection pool for Loader: "
							+ e1.toString());
			e1.printStackTrace(System.out);
			return;
		}

	}

	@Override
	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
	 */
	public int insert(String table, String key, HashMap<String, ByteIterator> values, boolean insertImage, int imageSize) {
		com.mongodb.DB db = null;
		WriteResult res = null;
		try {
			//get the appropriate database
			db = mongo.getDB(database);
			//ensure order
			db.requestStart();
			//appropriate table - collection
			DBCollection collection = db.getCollection(table);
			//create the row-object-document
			//need to insert key as integer else the sorting based on id for topk s wont happen properly

			DBObject r = new BasicDBObject().append("_id", Integer.parseInt(key));
			for(String k: values.keySet()) {
				r.put(k, values.get(k).toString());
			}

			if(table.equalsIgnoreCase("users")){
				//ArrayList x = new ArrayList();
				r.put("ConfFriends",new ArrayList<Integer>());
				r.put("PendFriends",new ArrayList<Integer>());
			}


			if(table.equalsIgnoreCase("users") && insertImage){
				//insert picture separately
				//create one gridFS for the datastore
				//Save image into database
				File file = new File(imagepath+"userpic"+imageSize+".bmp");
				int size = (int)file.length();
				byte[] buffer = new byte[size];
				FileInputStream in = new FileInputStream(file);
				in.read(buffer);
				in.close();
				GridFS fs = new GridFS( db, "photos" );
				GridFSInputFile in1 = fs.createFile( buffer );
				in1.save();
				r.put("imageid", in1.getId());

				//create the thumbnail image and save it
				file = new File(imagepath+"userpic1.bmp");
				size = (int)file.length();
				buffer = new byte[size];
				in = new FileInputStream(file);
				in.read(buffer);
				in.close();
				fs = new GridFS( db, "thumbnails" );
				in1 = fs.createFile( buffer );
				in1.save();
				r.put("thumbid", in1.getId());
				//Find saved image
				/*fs = new GridFS( db, "photos" );
		        DBObject tmp = new BasicDBObject( "_id" , r.get("imageid") );;
		        GridFSDBFile out = fs.findOne(tmp);
		        //Save loaded image from database into new image file
		        FileOutputStream outputImage = new FileOutputStream("out.bmp");
		        out.writeTo( outputImage );
		        outputImage.close();*/
			}
			res = collection.insert(r,writeConcern);
			/*
			// test to see if inserted - search query
			BasicDBObject searchQuery = new BasicDBObject();
			searchQuery.put("_id", Integer.parseInt(key));
			// query it
			DBCursor cursor = collection.find(searchQuery);
			// loop over the cursor and display the retrieved result
			while (cursor.hasNext()) {
				System.out.println(cursor.next());			      
			}
			return 0;
			 */	
		} catch (Exception e) {
			System.out.println(e.toString());
			return -1;
		} finally {
			if (db!=null)
			{
				db.requestDone();
			}
		}
		return res.getError() == null ? 0 : -1;
	}

	
	
	@Override
	public int getUserProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {

		int retVal = 0;
		if(requesterID < 0 || profileOwnerID < 0)
			return -1;

		com.mongodb.DB db=null;
		try {
			db = mongo.getDB(database);
			db.requestStart();						
			DBCollection collection = db.getCollection("users");
			DBObject q = new BasicDBObject().append("_id", profileOwnerID);		

			DBObject queryResult = null;
			queryResult = collection.findOne(q);
			String x = queryResult.get("ConfFriends").toString();
			int frndCount = 0;
			if(x.equals("") || (!x.equals("") && (x.substring(2, x.length()-1)).equals("")))
				frndCount = 0;
			else{
				x = x.substring(2, x.length()-1);
				frndCount = x.split(",").length;
			}

			int pendCount = 0;
			if(requesterID == profileOwnerID){
				x = queryResult.get("PendFriends").toString();
				if(x.equals("") || (!x.equals("") && (x.substring(2, x.length()-1)).equals("")))
					pendCount = 0;
				else{
					x = x.substring(2, x.length()-1);
					pendCount = x.split(",").length;
				}
			}

			//find number of resources for the user
			DBCollection resCollection = db.getCollection("resources");
			DBObject res = new BasicDBObject().append("walluserid", Integer.toString(profileOwnerID));		
			DBCursor resQueryResult = null;
			resQueryResult = resCollection.find(res);
			int resCount = resQueryResult.count();
			resQueryResult.close();

			if (queryResult != null) {
				//remove the ConfFriends and PendFriends and Resources
				//replace them with counts
				queryResult.removeField("ConfFriends");
				queryResult.removeField("PendFriends");
				result.putAll(queryResult.toMap());
				result.put("FriendCount", new StringByteIterator(Integer.toString(frndCount))) ;
				if(requesterID == profileOwnerID){
					result.put("PendingCount", new StringByteIterator(Integer.toString(pendCount))) ;
				}
				result.put("ResourceCount", new StringByteIterator(Integer.toString(resCount))) ;
			}

			if(insertImage){				GridFS fs = new GridFS( db, "photos" );
				DBObject tmp = new BasicDBObject( "_id" , queryResult.get("imageid") );
				GridFSDBFile out = fs.findOne( tmp );
				if(testMode){
					//Save loaded image from database into new image file
					FileOutputStream outputImage = new FileOutputStream(profileOwnerID+"-mprofimage.bmp");
					out.writeTo( outputImage );
					outputImage.close();
				}
				result.put("pic", new StringByteIterator(out.getInputStream().toString()));
			}


		} catch (Exception e) {
			System.out.println(e.toString());
			retVal = -1;
		}
		finally
		{
			if (db!=null)
			{
				db.requestDone();
			}
		}
		return retVal;
	}

	@Override
	public int getListOfFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
		int retVal = 0;
		if(requesterID < 0 || profileOwnerID < 0)
			return -1;

		//first get all confirmed friendids for profileOwnerID
		com.mongodb.DB db=null;

		try {
			db = mongo.getDB(database);
			db.requestStart();						
			DBCollection collection = db.getCollection("users");
			DBObject q = new BasicDBObject().append("_id", profileOwnerID);		

			DBObject queryResult = null;
			queryResult = collection.findOne(q);

			String x = queryResult.get("ConfFriends").toString();

			if(!x.equals("")){
				x = x.substring(2, x.length()-1);
				if(!x.equals("")){
					String friendIds[] = x.split(",");
					BasicDBObject query = new BasicDBObject();
					List<Integer> list = new ArrayList<Integer>();
					if(!friendListReq){
						for(int i=0; i<friendIds.length; i++){
							HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
							DBObject frnd = new BasicDBObject().append("_id", Integer.parseInt(friendIds[i].trim()));
							DBObject frndQueryResult = null;
							DBObject fieldsToReturn = new BasicDBObject();
							boolean returnAllFields = fields == null;
							if (!returnAllFields) {
								Iterator<String> iter = fields.iterator();
								while (iter.hasNext()) {
									fieldsToReturn.put(iter.next(), 1);
								}
								frndQueryResult = collection.findOne(frnd, fieldsToReturn);
							} else {					
								frndQueryResult = collection.findOne(frnd);
							}

							if (frndQueryResult != null) {
								frndQueryResult.removeField("ConfFriends");
								frndQueryResult.removeField("PendFriends");
								vals.putAll(frndQueryResult.toMap());
							}	
							//needed to do this so the coreworkload will not need to know the datastore typr
							if(vals.get("_id") != null){
								String tmp = vals.get("_id")+"";
								vals.remove("_id");
								vals.put("USERID",new StringByteIterator(tmp));
							}

							if(insertImage){
								GridFS fs = new GridFS( db, "thumbnails" );
								DBObject tmp = new BasicDBObject( "_id" , frndQueryResult.get("thumbid") );
								GridFSDBFile out = fs.findOne( tmp );
								vals.put("tpic", new StringByteIterator(out.getInputStream().toString()));
								if(testMode){
									//Save loaded image from database into new image file
									FileOutputStream outputImage = new FileOutputStream(profileOwnerID+"-"+i+"-mthumbimage.bmp");
									out.writeTo( outputImage );
									outputImage.close();
								}
							}
							result.add(vals);

						}
					}else if(friendListReq){//retrive one list
						for(int i=0; i<friendIds.length; i++){
							//put all in one list and retrieve instead of retrieving one by one
							list.add(Integer.parseInt(friendIds[i].trim()));
						}
						query.put("_id", new BasicDBObject("$in", list));
						DBCursor cursor = collection.find(query);

						int cnt=0;
						while(cursor.hasNext()) {
							cnt++;
							//System.out.println(cursor.next());
							HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();

							vals.putAll(cursor.next().toMap());
							vals.remove("ConfFriends");
							vals.remove("PendFriends");
							//needed to do this so the coreworkload will not need to know the datastore typr
							if(vals.get("_id") != null){
								String tmp = vals.get("_id")+"";
								vals.remove("_id");
								vals.put("USERID",new StringByteIterator(tmp));
							}
							if(insertImage){
								GridFS fs = new GridFS( db, "thumbnails" );
								DBObject tmp = new BasicDBObject( "_id" , vals.get("thumbid") );
								GridFSDBFile out = fs.findOne( tmp );
								vals.put("tpic", new StringByteIterator(out.getInputStream().toString()));
								if(testMode){
									//Save loaded image from database into new image file
									FileOutputStream outputImage = new FileOutputStream(profileOwnerID+"-"+cnt+"-mthumbimage.bmp");
									out.writeTo( outputImage );
									outputImage.close();
								}
							}
							result.add(vals);
						}
						cursor.close();
					}
				}
			}

		} catch (Exception e) {
			System.out.println(e.toString());
			retVal = -1;
		}
		finally
		{
			if (db!=null)
			{
				db.requestDone();
			}
		}
		return retVal;

	}

	@Override
	public int viewPendingRequests(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> values, boolean insertImage, boolean testMode) {
		int retVal = 0;
		if(profileOwnerID < 0)
			return -1;

		//first get all pending friendids for profileOwnerID
		com.mongodb.DB db=null;

		try {
			db = mongo.getDB(database);
			db.requestStart();						
			DBCollection collection = db.getCollection("users");
			DBObject q = new BasicDBObject().append("_id", profileOwnerID);		

			DBObject queryResult = null;
			queryResult = collection.findOne(q);

			String x = queryResult.get("PendFriends").toString();
			if(!x.equals("")){
				x = x.substring(2, x.length()-1);
				if(!x.equals("")){
					String friendIds[] = x.split(",");
					BasicDBObject query = new BasicDBObject();
					List<Integer> list = new ArrayList<Integer>();
					if(!friendListReq){
						for(int i=0; i<friendIds.length; i++){
							HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
							DBObject frnd = new BasicDBObject().append("_id", Integer.parseInt(friendIds[i].trim()));
							DBObject frndQueryResult = null;				
							frndQueryResult = collection.findOne(frnd);

							if (frndQueryResult != null) {
								frndQueryResult.removeField("ConfFriends");
								frndQueryResult.removeField("PendFriends");
								vals.putAll(frndQueryResult.toMap());
							}	
							if(vals.get("_id") != null){
								String tmp = vals.get("_id")+"";
								vals.remove("_id");
								vals.put("USERID",new StringByteIterator(tmp));
							}

							if(insertImage){
								GridFS fs = new GridFS( db, "photos" );
								DBObject tmp = new BasicDBObject( "_id" , frndQueryResult.get("imageid") );
								GridFSDBFile out = fs.findOne( tmp );
								vals.put("pic", new StringByteIterator(out.getInputStream().toString()));
								if(testMode){
									//Save loaded image from database into new image file
									FileOutputStream outputImage = new FileOutputStream(profileOwnerID+"-"+i+"-mimage.bmp");
									out.writeTo( outputImage );
									outputImage.close();
								}
							}

							values.add(vals);
						}
					}else if(friendListReq){//retrive one list
						for(int i=0; i<friendIds.length; i++){
							//put all in one list and retrieve instead of retrieving one by one
							list.add(Integer.parseInt(friendIds[i].trim()));
						}
						query.put("_id", new BasicDBObject("$in", list));
						DBCursor cursor = collection.find(query);
						int cnt =0;
						while(cursor.hasNext()) {
							cnt++;
							//System.out.println(cursor.next());
							HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
							vals.putAll(cursor.next().toMap());
							vals.remove("PendFriends");
							vals.remove("ConfFriends");
							//needed to do this so the coreworkload will not need to know the datastore typr
							if(vals.get("_id") != null){
								String tmp = vals.get("_id")+"";
								vals.remove("_id");
								vals.put("USERID",new StringByteIterator(tmp));
							}
							if(insertImage){
								GridFS fs = new GridFS( db, "thumbnails" );
								DBObject tmp = new BasicDBObject( "_id" , vals.get("thumbid") );
								GridFSDBFile out = fs.findOne( tmp );
								vals.put("tpic", new StringByteIterator(out.getInputStream().toString()));
								if(testMode){
									//Save loaded image from database into new image file
									FileOutputStream outputImage = new FileOutputStream(profileOwnerID+"-"+cnt+"-mthumbimage.bmp");
									out.writeTo( outputImage );
									outputImage.close();
								}
							}
							values.add(vals);
						}
						cursor.close();
					}

				}

			}

		} catch (Exception e) {
			System.out.println(e.toString());
			retVal = -1;
		}
		finally
		{
			if (db!=null)
			{
				db.requestDone();
			}
		}
		return retVal;

	}

	@Override
	public int acceptFriendRequest(int invitorID, int inviteeID) {
		//delete from pending of the invitee
		//add to confirmed of both invitee and invitor
		int retVal = 0;
		if(invitorID < 0 || inviteeID < 0)
			return -1;

		com.mongodb.DB db = null;
		try {
			db = mongo.getDB(database);
			db.requestStart();
			DBCollection collection = db.getCollection("users");
			DBObject q = new BasicDBObject().append("_id", inviteeID);

			//pull out of invitees pending
			BasicDBObject updateCommand = new BasicDBObject();
			updateCommand.put( "$pull", new BasicDBObject( "PendFriends",invitorID ) );
			WriteResult res = collection.update( q, updateCommand, false, false, writeConcern );

			//add to invitees confirmed
			updateCommand = new BasicDBObject();
			updateCommand.put( "$push", new BasicDBObject( "ConfFriends",invitorID ) );
			res = collection.update( q, updateCommand, false, false, writeConcern );



			//add to invitore confirmed
			q = new BasicDBObject().append("_id", invitorID);
			updateCommand = new BasicDBObject();
			updateCommand.put( "$push", new BasicDBObject( "ConfFriends",inviteeID ) );
			res = collection.update( q, updateCommand, false, false, writeConcern );
			db.requestDone();
			return res.getN() == 1 ? 0 : -1;

		} catch (Exception e) {
			System.out.println(e.toString());
			retVal = -1;
		} finally {
			if (db!=null)
			{
				db.requestDone();
			}
		}
		return retVal;

	}

	@Override
	public int rejectFriendRequest(int invitorID, int inviteeID) {
		//remove from pending of invitee
		int retVal = 0;
		if(invitorID < 0 || inviteeID < 0)
			return -1;

		com.mongodb.DB db = null;
		try {
			db = mongo.getDB(database);
			db.requestStart();
			DBCollection collection = db.getCollection("users");
			DBObject q = new BasicDBObject().append("_id", inviteeID);


			//pull out of invitees pending
			BasicDBObject updateCommand = new BasicDBObject();
			updateCommand.put( "$pull", new BasicDBObject( "PendFriends",invitorID ) );
			WriteResult res = collection.update( q, updateCommand, false, false, writeConcern );
			db.requestDone();
			return res.getN() == 1 ? 0 : -1;
		} catch (Exception e) {
			System.out.println(e.toString());
			retVal = -1;
		} finally {
			if (db!=null)
			{
				db.requestDone();
			}
		}
		return retVal;
	}

	@Override
	public int inviteFriends(int invitorID, int inviteeID) {
		//add to pending for the invitee
		int retVal = 0;
		if(invitorID < 0 || inviteeID < 0)
			return -1;
		com.mongodb.DB db = null;
		try {
			db = mongo.getDB(database);

			db.requestStart();
			DBCollection collection = db.getCollection("users");
			DBObject q = new BasicDBObject().append("_id", inviteeID);

			BasicDBObject updateCommand = new BasicDBObject();
			updateCommand.put( "$push", new BasicDBObject( "PendFriends",invitorID ) );
			WriteResult res = collection.update( q, updateCommand, false, false, writeConcern );
		} catch (Exception e) {
			System.out.println(e.toString());
			retVal = -1;
		} finally {
			if (db!=null)
			{
				db.requestDone();
			}
		}
		return retVal;
	}

	@Override
	public int getTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		int retVal = 0;
		if(profileOwnerID < 0 || requesterID < 0 || k < 0)
			return -1;

		com.mongodb.DB db=null;

		try {
			db = mongo.getDB(database);
			db.requestStart();						
			DBCollection collection = db.getCollection("resources");
			//find all resources that belong to profileOwnerID
			//sort them by _id desc coz we want latest ones and get the top k
			DBObject q = new BasicDBObject().append("walluserid", Integer.toString(profileOwnerID));
			DBCursor queryResult = null;
			queryResult = collection.find(q);
			//DBObject s = new BasicDBObject().append("_id", -1); //desc
			DBObject s = new BasicDBObject(); //desc
			s.put("_id", -1);
			queryResult = queryResult.sort(s);
			queryResult = queryResult.limit(k);
			Iterator it = queryResult.iterator();
			while(it.hasNext()){
				HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
				DBObject oneRes = new BasicDBObject();
				oneRes.putAll((DBObject) it.next());
				vals.putAll(oneRes.toMap());
				//needed to do this so the coreworkload will not need to know the datastore typr
				if(vals.get("_id") != null){
					String tmp = vals.get("_id")+"";
					vals.remove("_id");
					vals.put("RID",new StringByteIterator(tmp));
				}
				if(vals.get("walluserid") != null){
					String tmp = vals.get("walluserid")+"";
					vals.remove("walluserid");
					vals.put("WALLUSERID",new StringByteIterator(tmp));
				}
				result.add(vals);
			}
			queryResult.close();


		} catch (Exception e) {
			System.out.println(e.toString());
			retVal = -1;
		}
		finally
		{
			if (db!=null)
			{
				db.requestDone();
			}
		}
		return retVal;

	}


	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		int retVal = 0;
		if(creatorID < 0)
			return -1;

		com.mongodb.DB db=null;

		try {
			db = mongo.getDB(database);
			db.requestStart();						
			DBCollection collection = db.getCollection("resources");
			//find all resources that belong to profileOwnerID
			//sort them by _id desc coz we want latest ones and get the top k
			DBObject q = new BasicDBObject().append("creatorid", Integer.toString(creatorID));
			DBCursor queryResult = null;
			queryResult = collection.find(q);
			//DBObject s = new BasicDBObject().append("_id", -1); //desc
			DBObject s = new BasicDBObject(); //desc
			s.put("_id", -1);
			queryResult = queryResult.sort(s);
			Iterator it = queryResult.iterator();
			while(it.hasNext()){
				HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
				DBObject oneRes = new BasicDBObject();
				oneRes.putAll((DBObject) it.next());
				vals.putAll(oneRes.toMap());
				//needed to do this so the coreworkload will not need to know the datastore typr
				if(vals.get("_id") != null){
					String tmp = vals.get("_id")+"";
					vals.remove("_id");
					vals.put("RID",new StringByteIterator(tmp));
				}
				if(vals.get("creatorid") != null){
					String tmp = vals.get("creatorid")+"";
					vals.remove("creatorid");
					vals.put("CREATORID",new StringByteIterator(tmp));
				}
				result.add(vals);
			}
			queryResult.close();


		} catch (Exception e) {
			System.out.println(e.toString());
			retVal = -1;
		}
		finally
		{
			if (db!=null)
			{
				db.requestDone();
			}
		}
		return retVal;

	}


	@Override
	public int getResourceComments(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		int retVal = 0;
		if(profileOwnerID < 0 || requesterID < 0 || resourceID < 0)
			return -1;

		com.mongodb.DB db=null;

		try {
			db = mongo.getDB(database);
			db.requestStart();		
			if(!manipulationArray){
				DBCollection collection = db.getCollection("manipulation");
				//find all resources that belong to profileOwnerID
				//sort them by _id desc coz we want latest ones and get the top k
				DBObject q = new BasicDBObject().append("rid", Integer.toString(resourceID));
				DBCursor queryResult = null;
				queryResult = collection.find(q);
				Iterator<DBObject> it = queryResult.iterator();
				while(it.hasNext()){
					HashMap<String, ByteIterator> vals = new HashMap<String, ByteIterator>();
					DBObject oneRes = new BasicDBObject();
					oneRes.putAll((DBObject) it.next());
					vals.putAll(oneRes.toMap());
					result.add(vals);
				}
				queryResult.close();
			}else{
				DBCollection collection = db.getCollection("resources");
				DBObject q = new BasicDBObject().append("_id", resourceID);		
				DBObject queryResult = null;
				queryResult = collection.findOne(q);
				if(queryResult.get("Manipulations") != ""){
					ArrayList<DBObject> mans = (ArrayList<DBObject>) queryResult.get("Manipulations");
					for(int i=0; i<mans.size();i++){
						result.add((HashMap<String, ByteIterator>) mans.get(i).toMap());
					}
				}
			}

		} catch (Exception e) {
			System.out.println(e.toString());
			retVal = -1;
		}
		finally
		{
			if (db!=null)
			{
				db.requestDone();
			}
		}
		return retVal;
	}
	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID) {
		int retVal = 0;
		if(profileOwnerID < 0 || commentCreatorID < 0 || resourceID < 0)
			return -1;
		//create a new document
		com.mongodb.DB db = null;
		try {
			//get the appropriate database
			db = mongo.getDB(database);
			db.requestStart();
			if(!manipulationArray){ //consider a separate manipoulations table
				DBCollection collection = db.getCollection("manipulation");
				//create the row-object-document
				//need to insert key as integer else the sorting based on id for topk s wont happen properly
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				values.put("creatorid",new StringByteIterator(Integer.toString(profileOwnerID)));
				values.put("rid", new StringByteIterator(Integer.toString(resourceID)));
				values.put("modifierid", new StringByteIterator(Integer.toString(commentCreatorID)));
				values.put("timestamp", new StringByteIterator("datehihi"));
				values.put("type", new StringByteIterator("post"));
				values.put("content", new StringByteIterator("1234"));

				DBObject r = new BasicDBObject();
				for(String k: values.keySet()) {
					r.put(k, values.get(k).toString());
				}

				WriteResult res = collection.insert(r,writeConcern);
				return res.getError() == null ? 0 : -1;
			}else{
				//second approach - store manipulations as elemnets in an array for resource
				HashMap<String, String> sVals = new HashMap<String, String>();
				sVals.put("creatorid",Integer.toString(profileOwnerID));
				sVals.put("rid", Integer.toString(resourceID));
				sVals.put("modifierid", Integer.toString(commentCreatorID));
				sVals.put("timestamp", "datehihi");
				sVals.put("type", "post");
				sVals.put("content", "1234");
				DBCollection collection = db.getCollection("resources");
				DBObject q = new BasicDBObject().append("_id", resourceID);

				//pull out of invitees pending
				BasicDBObject updateCommand = new BasicDBObject();
				updateCommand.put( "$push", new BasicDBObject( "Manipulations",sVals ) );
				WriteResult res = collection.update( q, updateCommand, false, false, writeConcern );
				db.requestDone();
				return res.getError() == null ? 0 : -1;
			}

		} catch (Exception e) {
			System.out.println(e.toString());
			retVal = -1;
		} finally {
			if (db!=null)
			{
				db.requestDone();
			}
		}
		return retVal;
	}


	@Override
	public int unFriendFriend(int friendid1, int friendid2) {
		//delete from both their confFriends
		int retVal = 0;
		if(friendid1 < 0 || friendid2 < 0)
			return -1;

		com.mongodb.DB db = null;
		try {
			db = mongo.getDB(database);
			db.requestStart();
			DBCollection collection = db.getCollection("users");
			DBObject q = new BasicDBObject().append("_id", friendid1);

			//pull out of friend1 
			BasicDBObject updateCommand = new BasicDBObject();
			updateCommand.put( "$pull", new BasicDBObject( "ConfFriends",friendid2 ) );
			WriteResult res = collection.update( q, updateCommand, false, false, writeConcern );
			if(res.getN() !=1)
				return -1;


			q = new BasicDBObject().append("_id", friendid2);
			//pull out of friendid2
			updateCommand = new BasicDBObject();
			updateCommand.put( "$pull", new BasicDBObject( "ConfFriends",friendid1 ) );
			res = collection.update( q, updateCommand, false, false, writeConcern );

			db.requestDone();
			return res.getN() == 1 ? 0 : -1;

		} catch (Exception e) {
			System.out.println(e.toString());
			retVal = -1;
		} finally {
			if (db!=null)
			{
				db.requestDone();
			}
		}
		return retVal;
	}

	@Override
	public HashMap<String, String> getInitialStats() {

		HashMap<String, String> stats = new HashMap<String, String>();
		com.mongodb.DB db=null;
		try {
			db = mongo.getDB(database);
			db.requestStart();						
			//get the number of users
			DBCollection collection = db.getCollection("users");
			DBCursor users = collection.find();
			int usercnt = users.count();
			users.close();
			stats.put("usercount", Integer.toString(usercnt));

			//find user offset
			DBObject m = new BasicDBObject().append("_id", 1);
			DBCursor minUser = collection.find(m).limit(1);
			int offset = 0;
			if(minUser.hasNext())
				offset =  (Integer) minUser.next().toMap().get("_id");
			minUser.close();
			//get the number of friends per user
			DBObject q = new BasicDBObject().append("_id", offset);		
			DBObject queryResult = null;
			queryResult = collection.findOne(q);
			String x = queryResult.get("ConfFriends").toString();
			int frndCount = 0;
			if(x.equals("") || (!x.equals("") && (x.substring(2, x.length()-1)).equals("")))
				frndCount = 0;
			else{
				x = x.substring(2, x.length()-1);
				frndCount = x.split(",").length;
			}
			stats.put("avgfriendsperuser", Integer.toString(frndCount));

			x = queryResult.get("PendFriends").toString();
			int pendCount = 0;
			if(x.equals("") || (!x.equals("") && (x.substring(2, x.length()-1)).equals("")))
				pendCount = 0;
			else{
				x = x.substring(2, x.length()-1);
				pendCount = x.split(",").length;
			}
			stats.put("avgpendingperuser", Integer.toString(pendCount));


			//find number of resources for the user
			DBCollection resCollection = db.getCollection("resources");
			DBObject res = new BasicDBObject().append("creatorid", Integer.toString(offset));		
			DBCursor resQueryResult = null;
			resQueryResult = resCollection.find(res);
			int resCount = resQueryResult.count();
			resQueryResult.close();
			stats.put("resourcesperuser", Integer.toString(resCount));			
		}catch(Exception e){
			e.printStackTrace(System.out);
		}finally{
			if(db != null)
				db.requestDone();
		}
		return stats;
	}

	public void cleanup(boolean warmup) throws DBException
	{
		if(mongo!= null) mongo.close();
	}

	@Override
	public int CreateFriendship(int memberA, int memberB) {
		int retVal = acceptFriendRequest(memberA, memberB);
		return retVal;
	}

	@Override
	public int queryPendingFriendshipIds(int profileId,
			Vector<Integer> pendingFrnds) {
		int retVal = 0;
		com.mongodb.DB db=null;		
		try {
			db = mongo.getDB(database);
			db.requestStart();						
			DBCollection collection = db.getCollection("users");
			DBObject q = new BasicDBObject().append("_id", profileId);		
			DBObject queryResult = null;
			queryResult = collection.findOne(q);
			String x = queryResult.get("PendFriends").toString();
			if(!x.equals("")){
				x = x.substring(2, x.length()-1);
				if(!x.equals("")){
					String friendIds[] = x.split(",");
					for(int j=0; j<friendIds.length; j++)
						pendingFrnds.add(Integer.parseInt(friendIds[j].trim()));
				}
			}
		}catch(Exception e){
			e.printStackTrace(System.out);
			retVal = -1;
		}
		return retVal;
	}

	@Override
	public int queryConfirmedFriendshipIds(int profileId,
			Vector<Integer> confFrnds) {
		int retVal = 0;
		com.mongodb.DB db=null;		
		try {
			db = mongo.getDB(database);
			db.requestStart();						
			DBCollection collection = db.getCollection("users");
			DBObject q = new BasicDBObject().append("_id", profileId);		
			DBObject queryResult = null;
			queryResult = collection.findOne(q);
			String x = queryResult.get("ConfFriends").toString();
			if(!x.equals("")){
				x = x.substring(2, x.length()-1);
				if(!x.equals("")){
					String friendIds[] = x.split(",");
					for(int j=0; j<friendIds.length; j++)
						confFrnds.add(Integer.parseInt(friendIds[j].trim()));
				}
			}
		}catch(Exception e){
			e.printStackTrace(System.out);
			retVal = -1;
		}
		return retVal;
	}


	public void createSchema(Properties props){

		// drop all collections
		com.mongodb.DB db = null;
		try {
			// drop database and collections
			db = mongo.getDB(database);
			db.requestStart();
			DBCollection collection = db.getCollection("users");
			collection.drop();
			collection = db.getCollection("resources");
			collection.drop();
			collection = db.getCollection("manipulation");
			collection.drop();
			String bName = "photos";
			collection = db.getCollection( bName + ".files" );
			collection.drop();
			collection = db.getCollection( bName + ".chunks" );
			collection.drop();
			bName = "thumbnails";
			collection = db.getCollection( bName + ".files" );
			collection.drop();
			collection = db.getCollection( bName + ".chunks" );
			collection.drop();    

			if (Boolean
					.parseBoolean(props.getProperty(MONGODB_SHARDING_PROPERTY, MONGODB_SHARDING_PROPERTY_DEFAULT)) == true) {
				// enable sharding on the database in the admin user
				db = mongo.getDB("admin");
				BasicDBObject s = new BasicDBObject("enablesharding",
						"benchmark");
				CommandResult cr = db.command(s);
				// enable sharding on each collection
				cr = db.command(BasicDBObjectBuilder
						.start("shardCollection", "benchmark.users")
						.push("key").add("_id", 1).pop().get());
				if (Boolean.parseBoolean(props.getProperty(
						MONGODB_MANIPULATION_ARRAY_PROPERTY, MONGODB_MANIPULATION_ARRAY_PROPERTY_DEFAULT)) == false) {
					cr = db.command(BasicDBObjectBuilder
							.start("shardCollection", "benchmark.resources")
							.push("key").add("walluserid", 1).pop().get());
					cr = db.command(BasicDBObjectBuilder
							.start("shardCollection",
									"benchmark.manipulation").push("key")
									.add("rid", 1).pop().get());
				} else {
					cr = db.command(BasicDBObjectBuilder
							.start("shardCollection", "benchmark.resources")
							.push("key").add("_id", 1).pop().get());
				}
			}

			// create indexes on collection
			db = mongo.getDB(database);
			// collection = db.getCollection("users");
			collection = db.getCollection("resources");
			collection.createIndex(new BasicDBObject("walluserid", 1)); // create
			// index
			// on
			// "i",
			// ascending
			if (Boolean.parseBoolean(props.getProperty(MONGODB_MANIPULATION_ARRAY_PROPERTY,
					MONGODB_MANIPULATION_ARRAY_PROPERTY_DEFAULT)) == false) {
				collection = db.getCollection("manipulation");
				collection.createIndex(new BasicDBObject("rid", 1)); // create
				// index
				// on
				// "i",
				// ascending
			} else {
				collection.createIndex(new BasicDBObject(
						"resources.Manipulations", 1)); // create index on
				// "i", ascending
			}

		} catch (Exception e) {
			System.out.println(e.toString());
			return;
		} finally {
			if (db != null) {
				db.requestDone();
			}
		}
	}
}