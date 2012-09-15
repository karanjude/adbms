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


import java.io.*;
import java.sql.*;
import java.util.*;

import com.yahoo.ycsb.Client;

import edu.usc.bg.workloads.CoreWorkload;

/**
 * Reads the log record files and assigns them to the validation threads for being processed
 * merges the stats it gets from the validation threads
 * @author barahman
 *
 */
public class ValidationMainClass{
	
	public static final String DB_TENANT_PROPERTY = "tenant";
	public static final String DB_TENANT_PROPERTY_DEFAULT = "single"; //or multi
	public static final String VALIDATION_THREADS_PROPERTY = "validationthreads";
	public static final String VALIDATION_THREADS_PROPERTY_DEFAULT = "5";
	public static final String VALIDATION_BLOCK_PROPERTY = "validationblock";
	public static final String VALIDATION_BLOCK_PROPERTY_DEFAULT = "10";
	public static final String VALIDATION_APPROACH_PROPERTY = "validationapproach";
	public static final String VALIDATION_APPROACH_PROPERTY_DEFAULT = "interval"; //or RDBMS
	public static final String VALIDATION_DBURL_PROPERTY = "validation.url";
	public static final String VALIDATION_DBURL_PROPERTY_DEFAULT = "jdbc:oracle:thin:@localhost:1521:orcl";
	public static final String VALIDATION_DBUSER_PROPERTY = "validation.user";
	public static final String VALIDATION_DBUSER_PROPERTY_DEFAULT = "benchmark";
	public static final String VALIDATION_DBPWD_PROPERTY = "validation.passwd";
	public static final String VALIDATION_DBPWD_PROPERTY_DEFAULT = "111111";
	public static final String VALIDATION_DBDRIVER_PROPERTY = "validation.driver";
	public static final String VALIDATION_DBDRIVER_PROPERTY_DEFAULT = "oracle.jdbc.driver.OracleDriver";
	
	
	public static void buildValidationIndexes( Properties props) {
		String url = props.getProperty(VALIDATION_DBURL_PROPERTY,
				VALIDATION_DBURL_PROPERTY_DEFAULT);
		String user = props.getProperty(VALIDATION_DBUSER_PROPERTY, VALIDATION_DBUSER_PROPERTY_DEFAULT);
		String passwd = props.getProperty(VALIDATION_DBPWD_PROPERTY, VALIDATION_DBPWD_PROPERTY_DEFAULT);
		String driver = props.getProperty(VALIDATION_DBDRIVER_PROPERTY,
				VALIDATION_DBDRIVER_PROPERTY_DEFAULT);
		
		Connection conn = null;
		Statement stmt = null;
		int machineid =Integer.parseInt(props.getProperty(Client.MACHINE_ID_PROPERTY, Client.MACHINE_ID_PROPERTY_DEFAULT));

		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, passwd);
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}

		try {
			int count = 1;
			if(props.getProperty(DB_TENANT_PROPERTY,DB_TENANT_PROPERTY_DEFAULT).equalsIgnoreCase("single"))
				count = 1;
			else 
				count = Integer.parseInt(props.getProperty(Client.THREAD_CNT_PROPERTY, Client.THREAD_CNT_PROPERTY_DEFAULT));

			stmt = conn.createStatement();
			long startIdx = System.currentTimeMillis();
			for(int i=1; i<=count; i++){
				dropIndex(stmt, "TUPDATE"+machineid+"c"+i+"_IDX$$_start");
				dropIndex(stmt, "TUPDATE"+machineid+"c"+i+"_IDX$$_end");
				dropIndex(stmt, "TUPDATE"+machineid+"c"+i+"_IDX$$_resource");
				dropIndex(stmt, "TUPDATE"+machineid+"c"+i+"_IDX$$_optype");

				stmt.executeUpdate("CREATE INDEX TUPDATE"+machineid+"c"+i+"_IDX$$_start ON TUPDATE"+machineid+"c"+i+" (STARTTIME)"
						+ "COMPUTE STATISTICS NOLOGGING");
				stmt.executeUpdate("CREATE INDEX TUPDATE"+machineid+"c"+i+"_IDX$$_end ON TUPDATE"+machineid+"c"+i+" (ENDTIME)"
						+ "COMPUTE STATISTICS NOLOGGING");
				stmt.executeUpdate("CREATE INDEX TUPDATE"+machineid+"c"+i+"_IDX$$_resource ON TUPDATE"+machineid+"c"+i+" (RID)"
						+ "COMPUTE STATISTICS NOLOGGING");
				stmt.executeUpdate("CREATE INDEX TUPDATE"+machineid+"c"+i+"_IDX$$_optype ON TUPDATE"+machineid+"c"+i+" (OPTYPE)"
						+ "COMPUTE STATISTICS NOLOGGING");

				stmt.executeUpdate("analyze table tupdate"+machineid+"c"+i+" compute statistics");
				long endIdx = System.currentTimeMillis();
				System.out.println("\t Time to build validation index for" +machineid+" structures(ms):"
						+ (endIdx - startIdx));
			
			}

			
		} catch (Exception e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

	}

	public static void dumpFilesAndValidate(Properties props, HashMap<Integer, Vector<Integer>> seqTracker , HashMap<Integer, Vector<Integer>> staleSeqTracker, HashMap<String, Integer> stalenessStats, PrintWriter outpS, String dir){ 

		// open files for all threads one by one and read the records into the memory
		FileInputStream fstream;
		HashMap<String, resourceUpdateStat> updateStats = new HashMap<String, resourceUpdateStat>();
		
		//TODO: find a better way of passing
		HashMap<String, Integer> initCnt = CoreWorkload.initStats;		

		//create database connection
		Connection conn = null;
		Statement st = null;
		String url = props.getProperty(VALIDATION_DBURL_PROPERTY,
				VALIDATION_DBURL_PROPERTY_DEFAULT);
		String user = props.getProperty(VALIDATION_DBUSER_PROPERTY, VALIDATION_DBUSER_PROPERTY_DEFAULT);
		String passwd = props.getProperty(VALIDATION_DBPWD_PROPERTY, VALIDATION_DBPWD_PROPERTY_DEFAULT);
		String driver = props.getProperty(VALIDATION_DBDRIVER_PROPERTY,
				VALIDATION_DBDRIVER_PROPERTY_DEFAULT);

		int tableId = 1;  //if single tenant they will all dump into tread1 and tupdate1
		//else each will dump into their own table

		

		int pruned=0;
		int numUpdates = 0;
		int machineid =Integer.parseInt(props.getProperty(Client.MACHINE_ID_PROPERTY, Client.MACHINE_ID_PROPERTY_DEFAULT));
		String ratingMode = props.getProperty(Client.RATING_MODE_PROPERTY, Client.RATING_MODE_PROPERTY_DEFAULT);
		String tenant = props.getProperty(DB_TENANT_PROPERTY, DB_TENANT_PROPERTY_DEFAULT);
		int threadCount = Integer.parseInt(props.getProperty(Client.THREAD_CNT_PROPERTY, Client.THREAD_CNT_PROPERTY_DEFAULT));
		String approach = props.getProperty(VALIDATION_APPROACH_PROPERTY,
				VALIDATION_APPROACH_PROPERTY_DEFAULT);

		try {
			if(ratingMode.equals("true")){
				outpS.write("StartingValidation ");
				outpS.flush();
			}
			if(approach.equalsIgnoreCase("RDBMS")){
				ValidationMainClass.createValidationSchema(props);
				try {
					Class.forName(driver);
					conn = DriverManager.getConnection(url, user, passwd);
				} catch (Exception e) {
					e.printStackTrace(System.out);
				}
				st = conn.createStatement();
			}
			//read all the updates and update UpdateStats
			System.out.println("\t-- Starting to read update files...");
			long updateReadStart = System.currentTimeMillis();
			for(int i=0; i< threadCount; i++){
				fstream = new FileInputStream(dir+"//update"+machineid+"-"+i + ".txt");
				DataInputStream in = new DataInputStream(fstream);
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String line;
				// Read File Line By Line
				String[] tokens;
				while ((line = br.readLine()) != null) {
					numUpdates++;
					tokens = line.split(",");
					logObject record = new logObject(tokens[0], tokens[1], tokens[2],tokens[3], tokens[4], tokens[5], tokens[6], tokens[7], tokens[8]);
					if(updateStats.get(record.getMopType()+"-"+record.getRid()) != null){
						resourceUpdateStat newVal =updateStats.get(record.getMopType()+"-"+record.getRid());
						String tempValMinS = updateStats.get(record.getMopType()+"-"+record.getRid()).getMinStartTime();
						String tempValMaxE = updateStats.get(record.getMopType()+"-"+record.getRid()).getMaxEndTime();
						String tempValV = updateStats.get(record.getMopType()+"-"+record.getRid()).getFinalVal();

						//update min start time if needed
						if(Long.parseLong(tempValMinS) > Long.parseLong(record.getStarttime()))
							newVal.setMinStartTime(record.getStarttime());
						else
							newVal.setMinStartTime(tempValMinS);

						//update max end time if needed
						if(Long.parseLong(tempValMaxE) < Long.parseLong(record.getEndtime()))
							newVal.setMaxEndTime(record.getEndtime());
						else
							newVal.setMaxEndTime(tempValMaxE);

						if(record.getUpdatetype().equalsIgnoreCase("I"))
							newVal.setFinalVal((Integer.parseInt(tempValV)+1)+"");
						else
							newVal.setFinalVal((Integer.parseInt(tempValV)-1)+"");
						/*update structure
						updateStats.put(record.getMopType()+"-"+record.getRid(), newVal);*/
					}else{
						resourceUpdateStat newVal = new resourceUpdateStat();
						newVal.setMinStartTime(record.getStarttime());
						newVal.setMaxEndTime(record.getEndtime());
						if(record.getUpdatetype().equalsIgnoreCase("I"))
							newVal.setFinalVal("1");
						else
							newVal.setFinalVal("-1");

						updateStats.put(record.getMopType()+"-"+record.getRid(), newVal);
					}
					
					
					if(approach.equalsIgnoreCase("RDBMS")){
						//write update to database
						String sqlStr = "";
						if(tenant.equalsIgnoreCase("single"))
							tableId = 1;
						else
							tableId = Integer.parseInt(record.getThreadId())+1;
	
						sqlStr = "INSERT INTO tupdate"+machineid+"c"+tableId+" (opType, seqid, threadid, rid, starttime, endtime, numofupdate, updatetype) VALUES ("
								+ "'"
								+ record.getMopType()
								+ "', "
								+ record.getSeqId()
								+ ", "
								+ record.getThreadId()
								+ ", "
								+ record.getRid()
								+ ", "
								+ record.getStarttime()
								+ ", "
								+ record.getEndtime()
								+ ", "
								+ record.getValue()
								+ ", '"
								+ record.getUpdatetype() + "')";
						st.executeUpdate(sqlStr);
					}else{
						//add the interval to this resources interval tree
						Long updateTypeInLong = 0L;
						if(record.getUpdatetype().equals("I"))
							updateTypeInLong = 1L;
						else
							updateTypeInLong = -1L;
						updateStats.get(record.getMopType()+"-"+record.getRid()).addInterval(Long.parseLong(record.getStarttime()), Long.parseLong(record.getEndtime()), updateTypeInLong);
		                }
				}
				in.close();	
			}
			long updateReadEnd = System.currentTimeMillis();
			if(ratingMode.equals("true")){
				outpS.write("UpdatesInDB ");
				outpS.flush();
			}

			System.out.println("\t-- Done reading update files...elapsed time:"+(updateReadEnd-updateReadStart));
			if(approach.equalsIgnoreCase("RDBMS")){
				buildValidationIndexes(props);
			}
			System.out.println("\t-- Starting to read the read files...");
			updateReadStart = System.currentTimeMillis();

			//discard the reads that don have updates on their resources or those that happened before first update on their resources, or those happened after last update
			//read all read files
			int readToValidate=Integer.parseInt(props.getProperty(VALIDATION_BLOCK_PROPERTY, VALIDATION_BLOCK_PROPERTY_DEFAULT));
			int threadsToValidate = Integer.parseInt(props.getProperty(VALIDATION_THREADS_PROPERTY, VALIDATION_THREADS_PROPERTY_DEFAULT));
			int numReadOpsProcessed = 0; //these are the ones that were not pruned and needed to be processed
			int numStaleReadsReturned = 0;
			int validationTime=0;

			Vector<logObject> toBeProcessed = new Vector<logObject>();
			Vector<ValidationThread> vThreads = new Vector<ValidationThread>();
			for(int i=0; i<threadCount; i++){
				fstream = new FileInputStream(dir+"//read"+machineid+"-"+i + ".txt");
				DataInputStream in = new DataInputStream(fstream);
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				String line;
				// Read File Line By Line
				String[] tokens;
				while ((line = br.readLine()) != null) {
					tokens = line.split(",");
					logObject record = new logObject(tokens[0], tokens[1], tokens[2],tokens[3], tokens[4], tokens[5], tokens[6], tokens[7], "");
					// add sequence to sequences seen by this thread
					if (seqTracker.get(Integer.parseInt(record.getThreadId())) == null) {
						Vector<Integer> valLst = new Vector<Integer>();
						valLst.add(Integer.parseInt(record.getSeqId()));
						seqTracker.put(Integer.parseInt(record.getThreadId()), valLst);
					} else if (!seqTracker.get(Integer.parseInt(record.getThreadId())).contains(Integer.parseInt(record.getSeqId()))) {
						Vector<Integer> valLst = seqTracker.get(Integer.parseInt(record.getThreadId()));
						valLst.add(Integer.parseInt(record.getSeqId()));
						seqTracker.put(Integer.parseInt(record.getThreadId()), valLst);
					}

					if(updateStats.get(record.getMopType()+"-"+record.getRid()) != null){
						resourceUpdateStat updateLogStat = updateStats.get(record.getMopType()+"-"+record.getRid());
						//check if read before first update
						if(Long.parseLong(record.getEndtime()) < Long.parseLong(updateLogStat.getMinStartTime())){
							pruned++;
							int cmpVal = 0;
							if( initCnt.get(record.getMopType()+"-"+record.getRid()) == null  )
								cmpVal = 0;
							else
								cmpVal = initCnt.get(record.getMopType()+"-"+record.getRid());
							if(Integer.parseInt(record.getValue()) != cmpVal ){
								numStaleReadsReturned++;
								System.out.println("before update case: Data was stale for " + record.getMopType() + ": "
										+ record.getSeqId() + "-" + record.getThreadId() + "-" + record.getRid()
										+ ": Range is between "
										+ cmpVal
										+ "-"
										+ cmpVal
										+ " value Read is=" + record.getValue());
								if (staleSeqTracker.get(Integer.parseInt(record.getThreadId())) == null) {
									Vector<Integer> valLst = new Vector<Integer>();
									valLst.add(Integer.parseInt(record.getSeqId()));
									staleSeqTracker.put(Integer.parseInt(record.getThreadId()), valLst);
								} else if (!staleSeqTracker.get(Integer.parseInt(record.getThreadId())).contains(Integer.parseInt(record.getSeqId()))) {
									Vector<Integer> valLst = staleSeqTracker.get(Integer.parseInt(record.getThreadId()));
									valLst.add(Integer.parseInt(record.getSeqId()));
									staleSeqTracker.put(Integer.parseInt(record.getThreadId()), valLst);
								}
							}
						}else if(Long.parseLong(record.getStarttime()) > Long.parseLong(updateLogStat.getMaxEndTime())){ 
							//check if after last update
							pruned++;
							int inVal = 0;
							if(initCnt.get(record.getMopType()+"-"+record.getRid()) == null)
								inVal = 0;
							else
								inVal = initCnt.get(record.getMopType()+"-"+record.getRid());
							if(Integer.parseInt(record.getValue()) != (Integer.parseInt(updateLogStat.getFinalVal())+inVal)){
								numStaleReadsReturned++;
								System.out.println("after update case: Data was stale for " + record.getMopType() + ": "
										+ record.getSeqId() + "-" + record.getThreadId() + "-" + record.getRid()
										+ ": Range is between "
										+ (Integer.parseInt(updateLogStat.getFinalVal())+inVal)
										+ "-"
										+ (Integer.parseInt(updateLogStat.getFinalVal())+inVal)
										+ " value Read is=" + record.getValue());
								 
								if (staleSeqTracker.get(Integer.parseInt(record.getThreadId())) == null) {
									Vector<Integer> valLst = new Vector<Integer>();
									valLst.add(Integer.parseInt(record.getSeqId()));
									staleSeqTracker.put(Integer.parseInt(record.getThreadId()), valLst);
								} else if (!staleSeqTracker.get(Integer.parseInt(record.getThreadId())).contains(Integer.parseInt(record.getSeqId()))) {
									Vector<Integer> valLst = staleSeqTracker.get(Integer.parseInt(record.getThreadId()));
									valLst.add(Integer.parseInt(record.getSeqId()));
									staleSeqTracker.put(Integer.parseInt(record.getThreadId()), valLst);
								}
							}
						}else{
							if(readToValidate == toBeProcessed.size()){
								//we have all elements ready in vector to be processed
								if(vThreads.size() == threadsToValidate){
									//wait for the threads to be done
									for (Thread t : vThreads) {
										try {
											t.join();
											numReadOpsProcessed += ((ValidationThread) t).getNumOps();
											numStaleReadsReturned+= ((ValidationThread) t).getNumStaleOps();
											validationTime += ((ValidationThread) t).getValidationTime();
											HashMap<Integer, Vector<Integer>> staleSeqReturned = ((ValidationThread) t).getStaleSequences();
											Set<Integer> tids = staleSeqReturned.keySet();
											Iterator<Integer> it = tids.iterator();
											it = tids.iterator();
											while(it.hasNext()){
												Integer tid = (Integer)it.next();
												//get all the stale seqs seen by this thread
												Vector<Integer> seqsSeen = staleSeqReturned.get(tid);
												for(int j=0; j<seqsSeen.size(); j++){
													if(staleSeqTracker.get(tid) != null){
														if(!staleSeqTracker.get(tid).contains(seqsSeen.get(j))){
															//TODO: check this
															//( (Vector<Integer>)staleSeqTracker.get(tid) ).add(seqsSeen.get(j));
															Vector<Integer> vals = staleSeqTracker.get(tid);
															vals.add(seqsSeen.get(j));
															staleSeqTracker.put(tid, vals );
														}

													}else{
														Vector<Integer> vals = new Vector<Integer>();
														vals.add(seqsSeen.get(j));
														staleSeqTracker.put(tid, vals );
													}
												}
											}
										} catch (InterruptedException e) {
											e.printStackTrace(System.out);
										}
									}
									vThreads = new Vector<ValidationThread>();
								}
								ValidationThread newVThread = new ValidationThread(props,toBeProcessed, updateStats, initCnt);
								vThreads.add(newVThread);
								newVThread.start();
								toBeProcessed = new Vector<logObject>();
								toBeProcessed.add(record);
							}else{
								toBeProcessed.add(record);
							}

						}
					}else{ //no update available for this resource
						pruned++;
						//if(Integer.parseInt(record.getValue()) != 0){
						int cmpVal =0;
						if(initCnt.get(record.getMopType()+"-"+record.getRid()) == null)
							cmpVal = 0;
						else 
							cmpVal = initCnt.get(record.getMopType()+"-"+record.getRid());
						if(Integer.parseInt(record.getValue()) != cmpVal){
							numStaleReadsReturned++;
							System.out.println("never updated case: Data was stale for " + record.getMopType() + ": "
									+ record.getSeqId() + "-" + record.getThreadId() + "-" + record.getRid()
									+ ": Range is between "
									+ "0"
									+ "-"
									+ "0"
									+ " value Read is=" + record.getValue());
							if (staleSeqTracker.get(Integer.parseInt(record.getThreadId())) == null) {
								Vector<Integer> valLst = new Vector<Integer>();
								valLst.add(Integer.parseInt(record.getSeqId()));
								staleSeqTracker.put(Integer.parseInt(record.getThreadId()), valLst);
							} else if (!staleSeqTracker.get(Integer.parseInt(record.getThreadId())).contains(Integer.parseInt(record.getSeqId()))) {
								Vector<Integer> valLst = staleSeqTracker.get(Integer.parseInt(record.getThreadId()));
								valLst.add(Integer.parseInt(record.getSeqId()));
								staleSeqTracker.put(Integer.parseInt(record.getThreadId()), valLst);
							}
						}
					}
				}
			}
			if(ratingMode.equals("true")){
				outpS.write("DoneReadCycles ");
				outpS.flush();
			}
			//if any unproccessed reads process (create one thread for it)
			if(toBeProcessed.size() > 0){
				ValidationThread newVThread = new ValidationThread(props,toBeProcessed, updateStats, initCnt);
				vThreads.add(newVThread);
				newVThread.start();
				for(int u=0; u<vThreads.size(); u++){
					vThreads.get(u).join();
					numReadOpsProcessed += ((ValidationThread) 	vThreads.get(u)).getNumOps();
					numStaleReadsReturned+= ((ValidationThread) vThreads.get(u)).getNumStaleOps();
					validationTime += ((ValidationThread) vThreads.get(u)).getValidationTime();
					HashMap<Integer, Vector<Integer>> staleSeqReturned = ((ValidationThread) vThreads.get(u)).getStaleSequences();
					Set<Integer> tids = staleSeqReturned.keySet();
					Iterator<Integer> it = tids.iterator();
					while(it.hasNext()){
						int tid = (Integer)it.next();
						//get all the stale seqs seen by this thread
						if(staleSeqReturned.get(tid) != null){
							Vector<Integer> seqsSeen = staleSeqReturned.get(tid);
							for(int j=0; j<seqsSeen.size(); j++){
								if(staleSeqTracker.get(tid) != null){
									if(!staleSeqTracker.get(tid).contains(seqsSeen.get(j))){
										Vector<Integer> vals = staleSeqTracker.get(tid);
										vals.add(seqsSeen.get(j));
										staleSeqTracker.put(tid, vals );
									}

								}else{
									Vector<Integer> vals = new Vector<Integer>();
									vals.add(seqsSeen.get(j));
									staleSeqTracker.put(tid, vals );
								}
							}
						}else{
							System.out.println("Shouldnt happen!");
						}
					}
				}
				
				
				
			}
			if(ratingMode.equals("true")){
				outpS.write("DOneReadValidation ");
				outpS.flush();
			}
			updateReadEnd = System.currentTimeMillis();
			System.out.println("ReadValidationDuration(ms):"+(updateReadEnd-updateReadStart));
			//populate statistics
			stalenessStats.put("NumReadOps",numReadOpsProcessed+pruned);
			stalenessStats.put("NumProcessed",numReadOpsProcessed);
			stalenessStats.put("NumWriteOps",numUpdates);
			stalenessStats.put("NumStaleOps",numStaleReadsReturned);
			stalenessStats.put("NumPruned",pruned); 
			System.out.println("\t TotalReadOps = " + (numReadOpsProcessed+pruned) + " ,staleReadOps="
					+ numStaleReadsReturned + " ,staleness Perc (gran:user)="
					+ (((double) (numStaleReadsReturned)) / (numReadOpsProcessed+pruned)));

			Collection seqIdLst = seqTracker.values();
			Iterator it = seqIdLst.iterator();
			int totalSeq = 0;
			while (it.hasNext()) {
				totalSeq += ((List<Integer>) it.next()).size();
			}
			Collection staleSeqIdLst = staleSeqTracker.values();
			it = staleSeqIdLst.iterator();
			int totalStaleSeq = 0;
			while (it.hasNext()) {
				totalStaleSeq += ((List<Integer>) it.next()).size();
			}
			System.out.println("\t TotalSeqRead = " + totalSeq + " ,staleSeqRead="
					+ totalStaleSeq + " ,staleness Perc (gran:user)="
					+ (((double) (totalStaleSeq)) / totalSeq));
			stalenessStats.put("NumReadSessions",totalSeq);
			stalenessStats.put("NumStaleSessions",totalStaleSeq);
			stalenessStats.put("ValidationTime", validationTime);
			if(ratingMode.equals("true")){
				outpS.write("PopulateStats ");
				outpS.flush();
			}
			

		}catch (IOException e1) {
			e1.printStackTrace(System.out);
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		} finally{
			if(st!=null)
				try {
					st.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
			if(conn!=null)
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
		}

	}
	

	public static void dropSequence(Statement st, String seqName) {
		try {
			st.executeUpdate("drop sequence " + seqName);
		} catch (SQLException e) {
		}
	}

	public static void dropIndex(Statement st, String idxName) {
		try {
			st.executeUpdate("drop index " + idxName);
		} catch (SQLException e) {
		}
	}

	public static void dropTable(Statement st, String tableName) {
		try {
			st.executeUpdate("drop table " + tableName);
		} catch (SQLException e) {
		}
	}


	public static void createValidationSchema(Properties props) {
		int machineid = Integer.parseInt(props.getProperty(Client.MACHINE_ID_PROPERTY,Client.MACHINE_ID_PROPERTY_DEFAULT));
		Connection conn = null;
		Statement stmt = null;
		String url = props.getProperty(VALIDATION_DBURL_PROPERTY,
				VALIDATION_DBURL_PROPERTY_DEFAULT);
		String user = props.getProperty(VALIDATION_DBUSER_PROPERTY, VALIDATION_DBUSER_PROPERTY_DEFAULT);
		String passwd = props.getProperty(VALIDATION_DBPWD_PROPERTY, VALIDATION_DBPWD_PROPERTY_DEFAULT);
		String driver = props.getProperty(VALIDATION_DBDRIVER_PROPERTY,
				VALIDATION_DBDRIVER_PROPERTY_DEFAULT);
		
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, passwd);
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
		
		String tenant = props.getProperty(DB_TENANT_PROPERTY, DB_TENANT_PROPERTY_DEFAULT);
		int threadCount = Integer.parseInt(props.getProperty(Client.THREAD_CNT_PROPERTY, Client.THREAD_CNT_PROPERTY_DEFAULT));

		try {
			stmt = conn.createStatement();
			int count = 1;
			if(tenant.equalsIgnoreCase("single")) //create one read1 and one write1 table 
				count=1;
			else
				count = threadCount; //create a read and a write table per thread

			for(int i=1; i<=count; i++){
				dropSequence(stmt, "UPDATECNT"+machineid+"c"+i);
				dropTable(stmt, "TUPDATE"+machineid+"c"+i);
				stmt.executeUpdate("CREATE SEQUENCE  UPDATECNT"+machineid+"c"+i+"  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE");
				stmt.executeUpdate("CREATE TABLE TUPDATE"+machineid+"c"+i
						+ "(	OPTYPE VARCHAR(20), UPDATEID NUMBER,SEQID NUMBER,"
						+ "THREADID NUMBER,"
						+ "RID NUMBER, STARTTIME VARCHAR2(20),"
						+ "ENDTIME VARCHAR2(20), NUMOFUPDATE NUMBER, UPDATETYPE VARCHAR2(20)"
						+ ") NOLOGGING");

				stmt.executeUpdate("ALTER TABLE TUPDATE"+machineid+"c"+i+" MODIFY (UPDATEID NOT NULL ENABLE)");
				stmt.executeUpdate("ALTER TABLE TUPDATE"+machineid+"c"+i+" MODIFY (THREADID NOT NULL ENABLE)");
				stmt.executeUpdate("ALTER TABLE TUPDATE"+machineid+"c"+i+" MODIFY (RID NOT NULL ENABLE)");


				stmt.executeUpdate("CREATE OR REPLACE TRIGGER UPDATEINC"+machineid+"c"+i+" before insert on tupdate"+machineid+"c"+i+" "
						+ "for each row "
						+ "WHEN (new.updateid is null) begin "
						+ "select updateCnt"+i+".nextval into :new.updateid from dual;"
						+ "end;");
				stmt.executeUpdate("ALTER TRIGGER UPDATEINC"+machineid+"c"+i+" ENABLE");
			}

		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} finally {
			if (stmt != null){
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
			}
			
			if (conn != null){
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
			}
			
		}
	}

	
	public static void main(String[] args){
		
		HashMap<Integer, Vector<Integer>> seqTracker = new HashMap<Integer, Vector<Integer>>();
		HashMap<Integer, Vector<Integer>> staleSeqTracker = new HashMap<Integer, Vector<Integer>>();
		HashMap<String, Integer> stalenessStats = new HashMap<String, Integer>();
		//String dir = "C://BG//Tests//validation";
		String dir = "C:/BG/Tests/validation/4";
		Properties props = new Properties();
		props.setProperty("threadcount","1");
		props.setProperty("machineid","0");	
		//props.setProperty("validationapproach","RDBMS"); //can be interval
		props.setProperty("validationapproach","INTERVAL"); //can be interval
		if(props.getProperty(VALIDATION_APPROACH_PROPERTY, VALIDATION_APPROACH_PROPERTY_DEFAULT).equals("RDBMS"))
			createValidationSchema(props);
		dumpFilesAndValidate(props, seqTracker , staleSeqTracker, stalenessStats, null, dir);
		
	}

}






