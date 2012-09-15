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


package edu.usc.bg.validator;

//import intervalTree.Interval;

import java.sql.*;
import java.util.*;

import com.yahoo.ycsb.Client;

/**
 * The thread responsible for validating the read logs using the RDBMS or the interval tree created
 * @author barahman
 *
 */
public class ValidationThread extends Thread {
	int _numStaleOps=0;
	int _numOps=0;
	HashMap<Integer, Vector<Integer>> _staleSeqTracker = new HashMap<Integer, Vector<Integer>>();
	Vector<logObject> _toProcess;
	int _validationTime = 0;
	Properties _props;
	HashMap<String, resourceUpdateStat> _resUpdateDetails;
	HashMap<String , Integer> _initStats = new HashMap<String, Integer>();

	public ValidationThread(Properties props, Vector<logObject> toBeProcessed, HashMap<String, resourceUpdateStat> resUpdateDetails, HashMap<String, Integer> initStats) {
		_toProcess = toBeProcessed;
		_props = props;
		_resUpdateDetails = resUpdateDetails;
		_initStats = initStats;
		
	}

	public int getNumStaleOps() {
		return _numStaleOps;
	}

	public int getNumOps() {
		return _numOps;
	}

	public int getValidationTime() {
		return _validationTime;
	}

	public HashMap<Integer, Vector<Integer>> getStaleSequences(){
		return _staleSeqTracker;
	}

	public void run() {
		
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		long startValidation = 0, endValidation = 0;
		int inccnt = 0;   //overlapping and incrementing
		int deccnt = 0;   //overlapping and decrementing
		int numCompleted = 0; //previously completed ones
		

		String url = _props.getProperty(ValidationMainClass.VALIDATION_DBURL_PROPERTY,
				ValidationMainClass.VALIDATION_DBURL_PROPERTY_DEFAULT);
		String user = _props.getProperty(ValidationMainClass.VALIDATION_DBUSER_PROPERTY, ValidationMainClass.VALIDATION_DBUSER_PROPERTY_DEFAULT);
		String passwd = _props.getProperty(ValidationMainClass.VALIDATION_DBPWD_PROPERTY, ValidationMainClass.VALIDATION_DBPWD_PROPERTY_DEFAULT);
		String driver = _props.getProperty(ValidationMainClass.VALIDATION_DBDRIVER_PROPERTY,
				ValidationMainClass.VALIDATION_DBDRIVER_PROPERTY_DEFAULT);
		int tenant=0; //0 for single and 1 for multi
		int machineid =Integer.parseInt(_props.getProperty(Client.MACHINE_ID_PROPERTY, Client.MACHINE_ID_PROPERTY_DEFAULT));
		String approach = _props.getProperty(ValidationMainClass.VALIDATION_APPROACH_PROPERTY, ValidationMainClass.VALIDATION_APPROACH_PROPERTY_DEFAULT);
		if(_props.getProperty("tenant", "single").equalsIgnoreCase("single"))
			tenant = 0;
		else
			tenant = 1;
		int threadCount = Integer.parseInt(_props.getProperty(Client.THREAD_CNT_PROPERTY,Client.THREAD_CNT_PROPERTY_DEFAULT));
		
		
		if(approach.equalsIgnoreCase("RDBMS")){
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, passwd);
			stmt = conn.createStatement();
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
		}

		// get all the read operations
		try {
			
			startValidation = System.currentTimeMillis();
			//process every read in the vector passed to this thread
			for(int u=0; u<_toProcess.size(); u++) {
				_numOps++;
				logObject record = _toProcess.get(u);
				// get the resource id for the res
				int rid = Integer.parseInt(record.getRid());
				String start = record.getStarttime();
				String end = record.getEndtime();
				int val = Integer.parseInt(record.getValue());
				int seqid = Integer.parseInt(record.getSeqId());
				int threadid = Integer.parseInt(record.getThreadId());
				String opType = record.getMopType();
				
				if(approach.equalsIgnoreCase("RDBMS")){
					// find all the max number of updates for each thread,
					// for this rid which completed before the read started
					String query ="";
					if(tenant==0)
						query = "select * from tupdate"+machineid+"c1 where rid="+rid+" and opType='"+opType+"' and starttime<"+start+" and endtime<="+start;
					else{
						String union ="(";
						for(int i=1; i<=threadCount; i++){
							if(i!= threadCount)
								union +="select * from tupdate"+machineid+"c"+i+" UNION ALL ";
							else 
								union +="select * from tupdate"+machineid+"c"+i+") ";
						}
						query = "select * from "+union+" where rid="+rid+" and opType='"+opType+"' and starttime<"+start+" and endtime<="+start;
	
					}
					stmt = conn.createStatement();
					rs = stmt.executeQuery(query);
					if( _initStats.get(opType+"-"+rid) == null)
						numCompleted = 0;
					else 
						numCompleted = _initStats.get(opType+"-"+rid);
					while(rs.next()){
						if(rs.getString("updatetype").equalsIgnoreCase("I"))
							numCompleted++;
						else
							numCompleted--;
					}
					// find all those that were happening while the read was happening
					if(tenant==0)
						query = "select * from tupdate"+machineid+"c"+"1 where ((endtime<=" + end
						+ " and starttime>=" + start + ") " + "OR (starttime<"
						+ start + " and endtime>" + start + " and endtime<"
						+ end + ") " + "OR (starttime>" + start
						+ " and endtime>" + end + " and starttime<" + end
						+ ") " + "OR (starttime<" + start + " and endtime>"
						+ end + ")) and optype='" + opType + "' and rid=" + rid;
	
					else{
						String union ="(";
						for(int i=1; i<=threadCount; i++){
							if(i!= threadCount)
								union +="select * from tupdate"+machineid+"c"+i+" UNION ALL ";
							else 
								union +="select * from tupdate"+machineid+"c"+i+") ";
						}
						query = "select * from "+union+" where ((endtime<=" + end
								+ " and starttime>=" + start + ") " + "OR (starttime<"
								+ start + " and endtime>" + start + " and endtime<"
								+ end + ") " + "OR (starttime>" + start
								+ " and endtime>" + end + " and starttime<" + end
								+ ") " + "OR (starttime<" + start + " and endtime>"
								+ end + ")) and optype='" + opType + "' and rid=" + rid;	
					}
					if(rs != null) rs.close();	
					rs = stmt.executeQuery(query);
					inccnt = 0;
					deccnt = 0;
					while (rs.next()) {
						if (rs.getString("updatetype").equals("I"))
							inccnt++;
						else
							deccnt--;
					}
					
					if (rs != null)
						rs.close();
					if (stmt != null)
						stmt.close();
				}else{
					//query the interval tree
					  List<Interval<Long>> overlapResult=null;
					  //first get all the overlapping ones
					  overlapResult = _resUpdateDetails.get(opType+"-"+rid).queryIntervalTree(Long.parseLong(start), Long.parseLong(end));
					  // got through the overlapping ones and detect which ones are increementing and which are decrementing
					  //kissing is not overlapping
					  //for prev completed we need to find the min of read start time and all the overlapping updates
					  //Long minTime = Long.parseLong(start);
					  inccnt = 0;
					  deccnt = 0;
                      for (int j=0; j < overlapResult.size(); j++){
                    	 /* if(((Interval)(overlapResult.get(j))).getStart() < minTime)
                    		  minTime = ((Interval)(overlapResult.get(j))).getStart();*/
                          if(overlapResult.get(j).getData()== 1)
                        	  inccnt++;
                          else
                        	  deccnt--;
                      }
                      //query for prev completed till the start read time
                      //if update end kisses read start it is considered as already completed
                      List<Interval<Long>> completedResult=null;
                      //completedResult = _resUpdateDetails.get(opType+"-"+rid).queryIntervalTree(0, minTime);
                      completedResult = _resUpdateDetails.get(opType+"-"+rid).queryIntervalTree(0, Long.parseLong(start));
                      if( _initStats.get(opType+"-"+rid) == null)
						numCompleted = 0;
                      else 
						numCompleted = _initStats.get(opType+"-"+rid);
				
                      boolean hasDuplicate = false;
                      for (int j=0; j < completedResult.size(); j++){
                    	 hasDuplicate = false;
                    	  //check if already counted in overlapping intervals
                    	  //go thru all overlapping updates
                    	  /*for(int k=0; k<overlapResult.size(); k++){
                    		  if(completedResult.get(j).getStart() == overlapResult.get(k).getStart() && completedResult.get(j).getEnd() == overlapResult.get(k).getEnd()){
                    			//already been counted
                    			  hasDuplicate = true;
                    			  break;
                    		  }
                    		  
                    	  }*/
                    	 //check if the completed interval has the start of the read in it
                    	 //if it has it then it has already been counted in overlapping
                    	 if(completedResult.get(j).contains(Long.parseLong(start)))
                    		 hasDuplicate = true;
                    	 
                    	  if(!hasDuplicate){
	                          if(completedResult.get(j).getData()== 1)
	                        	  numCompleted++;
	                          else
	                        	  numCompleted--;
                    	  }
                    	 // System.out.println(numCompleted);
                    	  
                      }
				}
				// any of the overlapping ones can be either seen or not seen,
				// so the range would be as follows
				if (!(Math.min(numCompleted, (numCompleted + deccnt)) <= val && val <= Math
						.max(numCompleted, (numCompleted + inccnt)))) {
					_numStaleOps++;
					System.out.println("*Data was stale for " + opType + ": "
							+ seqid + "-" + threadid + "-" + rid
							+ ": Range is between "
							+ (Math.min(numCompleted, (numCompleted + deccnt)))
							+ "-"
							+ Math.max(numCompleted, (numCompleted + inccnt))
							+ " value Read is=" + val);
					if (_staleSeqTracker.get(threadid) == null) {
						Vector<Integer> valLst = new Vector<Integer>();
						valLst.add(seqid);
						_staleSeqTracker.put(threadid, valLst);
					} else if (!_staleSeqTracker.get(threadid).contains(seqid)) {
						Vector<Integer> valLst = _staleSeqTracker.get(threadid);
						valLst.add(seqid);
						_staleSeqTracker.put(threadid, valLst);
					}
				}				
			}
			endValidation = System.currentTimeMillis();
			_validationTime = (int)(endValidation-startValidation);

		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null) rs.close();
				if (stmt != null) 	stmt.close();
				if (conn != null)   conn.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}

		}

	}
}
