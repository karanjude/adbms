package voltdbDataStore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.voltcore.utils.PortGenerator;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.StringByteIterator;

public class voltdbClient extends DB {
	ServerThread server = null;
	VoltDB.Configuration config;
	int cnt1 = 0;
	int cnt2 = 0;
	int cnt3 = 0;
	int cntForViewProfile = 0, cntForListFriends = 0, pendingFriendsCount = 0;
	int topKCount = 0, rejectFriendCount = 0, acceptFriendcnt = 0,
			thawCount = 0;
	int createdResourceCount = 0;

	org.voltdb.client.Client client = null;
	Properties props = null;
	String path = "";

	public void init() {
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		//System.out.println("getCreatedResources");

		try {
			if (null == client) {
				client = ClientFactory.createClient();
				client.createConnection("localhost");
			}

			ClientResponse response;
			response = client.callProcedure("GetCreatedResources", creatorID);

			VoltTable results[] = response.getResults();
			VoltTable resultTable = results[0];
			for (int i = 0; i < resultTable.getRowCount(); i++) {
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				VoltTableRow row = resultTable.fetchRow(i);
				for (int j = 0; j < resultTable.getColumnCount(); j++) {

					if (j == 0 || j == 1 || j == 2) {
						String valueInt = row.get(j, VoltType.INTEGER)
								.toString();
						values.put(resultTable.getColumnName(j),
								new StringByteIterator(valueInt));
					} else {
						String valueString = row.getString(j);
						values.put(resultTable.getColumnName(j),
								new StringByteIterator(valueString));
					}
				}
				result.add(values);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				client.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return 0;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		// TODO Auto-generated method stub
		HashMap<String, String> stats = new HashMap<String, String>();
		//System.out.println("getInitialStats");
		try {
			if (null == client) {
				client = ClientFactory.createClient();
				client.createConnection("localhost");
			}

			ClientResponse response1 = client.callProcedure("SelectUserCount");
			VoltTable results[] = response1.getResults();
			VoltTable resultTable = results[0];

			VoltTableRow row = resultTable.fetchRow(0);
			stats.put("usercount", row.get(0, VoltType.INTEGER).toString());

			ClientResponse response2 = client.callProcedure("UserOffset");
			VoltTable results2[] = response2.getResults();
			VoltTable resultTable2 = results2[0];

			int offset;
			VoltTableRow row2 = resultTable2.fetchRow(0);
			offset = (Integer) row2.get(0, VoltType.INTEGER);

			ClientResponse response3 = client.callProcedure("ResourceCount",
					offset);
			VoltTable results3[] = response3.getResults();
			VoltTable resultTable3 = results3[0];

			VoltTableRow row3 = resultTable3.fetchRow(0);
			stats.put("resourcesperuser", row3.get(0, VoltType.INTEGER)
					.toString());

			ClientResponse response4 = client.callProcedure("FriendsPerUser",
					offset);
			VoltTable results4[] = response4.getResults();
			VoltTable resultTable4 = results4[0];

			VoltTableRow row4 = resultTable4.fetchRow(0);
			stats.put("avgfriendsperuser", row4.get(0, VoltType.INTEGER)
					.toString());

			ClientResponse response5 = client.callProcedure(
					"PendingFriendsPerUser", offset);
			VoltTable results5[] = response5.getResults();
			VoltTable resultTable5 = results5[0];

			VoltTableRow row5 = resultTable5.fetchRow(0);
			stats.put("avgpendingperuser", row5.get(0, VoltType.INTEGER)
					.toString());

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return stats;
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		//System.out.println("CreateFriendship");
		try {
			if (null == client) {
				client = ClientFactory.createClient();
				client.createConnection("localhost");
			}

			client.callProcedure("InsertFriends", friendid1, friendid2, 2);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ProcCallException e) {
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	public void createSchema(Properties props) {
		VoltProjectBuilder builder = new VoltProjectBuilder();
		try {

			builder.addLiteralSchema("CREATE TABLE Friendship ("
					+ "  userid1 INTEGER NOT NULL,"
					+ "  userid2 INTEGER NOT NULL,"
					+ "  status INTEGER NOT NULL,"
					+ "  PRIMARY KEY (userid1,userid2)" + "); ");

			if (Boolean.parseBoolean(props.getProperty(
					Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT))) {
				builder.addLiteralSchema("CREATE TABLE Users ("
						+ "  userid INTEGER NOT NULL,"
						+ "  username VARCHAR(255),"
						+ "  pw VARCHAR(255),"
						+ "  fname VARCHAR(255),"
						+ "  lname VARCHAR(255),"
						+ "  gender VARCHAR(255),"
						+ "  dob VARCHAR(255),"
						+ "  jdate VARCHAR(255),"
						+ "  ldate VARCHAR(255),"
						+ "  address VARCHAR(255),"
						+ "  email VARCHAR(255),"
						+ "  tel VARCHAR(255),"
						+ "  confirmedFriends INTEGER,"
						+ "  pendingFriends INTEGER,"
						+ "  resourceCount INTEGER,"
						+ "  pic VARBINARY(950000),"
						+ "  tpic VARBINARY(520000), "
						+ "  PRIMARY KEY (userid)"
						+ "); "
						+ "CREATE INDEX i2 ON Users (confirmedFriends,pendingFriends,resourceCount);");
			} else {
				builder.addLiteralSchema("CREATE TABLE Users ("
						+ "  userid INTEGER NOT NULL,"
						+ "  username VARCHAR(255),"
						+ "  pw VARCHAR(255),"
						+ "  fname VARCHAR(255),"
						+ "  lname VARCHAR(255),"
						+ "  gender VARCHAR(255),"
						+ "  dob VARCHAR(255),"
						+ "  jdate VARCHAR(255),"
						+ "  ldate VARCHAR(255),"
						+ "  address VARCHAR(255),"
						+ "  email VARCHAR(255),"
						+ "  tel VARCHAR(255),"
						+ "  confirmedFriends INTEGER,"
						+ "  pendingFriends INTEGER,"
						+ "  resourceCount INTEGER,"
						+ "  PRIMARY KEY (userid)"
						+ "); "
						+ "CREATE INDEX i2 ON Users (confirmedFriends,pendingFriends,resourceCount);");
			}

			builder.addLiteralSchema("CREATE TABLE Resource ("
					+ "  rid INTEGER NOT NULL," + "  creatorid INTEGER,"
					+ "  wallUserId INTEGER NOT NULL," + "  type VARCHAR(255),"
					+ "  body VARCHAR(255)," + "  doc VARCHAR(255),"
					+ "  PRIMARY KEY (rid)" + "); "
					+ "CREATE INDEX i3 ON Resource (wallUserId);");

			builder.addPartitionInfo("Resource", "wallUserId");

			builder.addLiteralSchema("CREATE TABLE Modify ("
					+ "  creatorid INTEGER," + "  rid INTEGER NOT NULL,"
					+ "  modifierid INTEGER," + "  timestamp VARCHAR(255),"
					+ "  type VARCHAR(255)," + "  content VARCHAR(255),"
					+ "); " + "CREATE INDEX i4 ON Modify (rid);");

			builder.addPartitionInfo("Modify", "rid");

			if (Boolean.parseBoolean(props.getProperty(
					Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT))) {
				builder.addProcedures(new Class<?>[] { InsertResources.class,
						InsertFriends.class, SelectUserCount.class,
						UserOffset.class, ResourceCount.class,
						FriendsPerUser.class, PendingFriendsPerUser.class,
						InsertUserPic.class, ProfileDetailsPic.class,
						ListFriendsPic.class, InsertPendingFriends.class,
						ViewTopKResources.class, DeleteFriendship.class,
						ViewFriendReqPic.class, AcceptFriend.class,
						ThawFriendship.class, GetCreatedResources.class,
						ViewCommentOnResource.class,
						PostCommentOnResource.class

				});
			} else {
				builder.addProcedures(new Class<?>[] { InsertUser.class,
						InsertResources.class, InsertFriends.class,
						SelectUserCount.class, UserOffset.class,
						ResourceCount.class, FriendsPerUser.class,
						PendingFriendsPerUser.class, ProfileDetails.class,
						ListFriends.class, InsertPendingFriends.class,
						ViewTopKResources.class, DeleteFriendship.class,
						ViewFriendReq.class, AcceptFriend.class,
						ThawFriendship.class, GetCreatedResources.class,
						ViewCommentOnResource.class,
						PostCommentOnResource.class

				});

			}

			boolean success = builder.compile(config.m_pathToCatalog, 2, 1, 0);
			assert success;
			System.out.println(builder.getPathToDeployment());
			System.out.println(config.m_pathToDeployment);
			MiscUtils.copyFile(builder.getPathToDeployment(),
					config.m_pathToDeployment);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void cleanup(boolean warmup) {
		try {

			if (client != null) {

				client.close();

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID,
			Vector<Integer> confirmedIds) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int insert(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage,
			int imageSize) {
		//System.out.println("insert");
		try {
			if (null == client) {
				client = ClientFactory.createClient();
				client.createConnection("localhost");
			}
			if (entitySet.equals("users")) {
				ArrayList<String> tempList = new ArrayList<String>();
				for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
					String field = entry.getValue().toString();
					tempList.add(field);
				}
				if (insertImage) {
					props = getProperties();
					path = props.getProperty(Client.IMAGE_PATH_PROPERTY);
					File image = new File(path + "userpic" + imageSize + ".bmp");
					FileInputStream fis = new FileInputStream(image);
					byte fileContent[] = new byte[(int) image.length()];
					fis.read(fileContent);

					File thumbimage = new File(path + "userpic1.bmp");
					FileInputStream fist = new FileInputStream(thumbimage);
					byte fileContent2[] = new byte[(int) image.length()];
					fist.read(fileContent2);
					client.callProcedure("InsertUserPic", entityPK,
							tempList.get(0), tempList.get(1), tempList.get(2),
							tempList.get(3), tempList.get(4), tempList.get(5),
							tempList.get(6), tempList.get(7), tempList.get(8),
							tempList.get(9), tempList.get(10), 0, 0, 0,
							fileContent, fileContent2);
				} else {
					client.callProcedure("InsertUser", entityPK,
							tempList.get(0), tempList.get(1), tempList.get(2),
							tempList.get(3), tempList.get(4), tempList.get(5),
							tempList.get(6), tempList.get(7), tempList.get(8),
							tempList.get(9), tempList.get(10), 0, 0, 0);
				}
			} else if (entitySet.equals("resources")) {

				ArrayList<String> tempList = new ArrayList<String>();
				for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
					String field = entry.getValue().toString();
					tempList.add(field);
				}

				client.callProcedure("InsertResources", entityPK,
						tempList.get(0), tempList.get(1), tempList.get(2),
						tempList.get(3), tempList.get(4));

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NullPointerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getUserProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		//System.out.println("getUserProfile");
		try {
			if (null == client) {
				client = ClientFactory.createClient();
				client.createConnection("localhost");
			}

			// confirmed friend count
			ClientResponse response1 = null;
			try {
				response1 = client.callProcedure("FriendsPerUser",
						profileOwnerID);
			} catch (NoConnectionsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			VoltTable results[] = response1.getResults();
			VoltTable resultTable = results[0];

			VoltTableRow row = resultTable.fetchRow(0);
			result.put("friendcount",
					new StringByteIterator(row.get(0, VoltType.INTEGER)
							.toString()));

			// pending friend count
			if (requesterID == profileOwnerID) {
				ClientResponse response2 = null;
				try {
					response2 = client.callProcedure("PendingFriendsPerUser",
							profileOwnerID);
				} catch (NoConnectionsException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				VoltTable results2[] = response2.getResults();
				VoltTable resultTable2 = results2[0];

				VoltTableRow row2 = resultTable2.fetchRow(0);
				result.put("pendingcount",
						new StringByteIterator(row2.get(0, VoltType.INTEGER)
								.toString()));

			}

			// resource count
			ClientResponse response3 = null;
			try {
				response3 = client.callProcedure("ResourceCount",
						profileOwnerID);
			} catch (NoConnectionsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			VoltTable results3[] = response3.getResults();
			VoltTable resultTable3 = results3[0];

			VoltTableRow row3 = resultTable3.fetchRow(0);
			result.put("resourcecount",
					new StringByteIterator(row3.get(0, VoltType.INTEGER)
							.toString()));

			// profile details
			ClientResponse response4 = null;
			if (insertImage) {
				try {
					response4 = client.callProcedure("ProfileDetailsPic",
							profileOwnerID);
				} catch (NoConnectionsException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				try {
					response4 = client.callProcedure("ProfileDetails",
							profileOwnerID);
				} catch (NoConnectionsException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			String valueInt;
			String valueString;
			VoltTable results4[] = response4.getResults();
			VoltTable resultTable4 = results4[0];

			int coloumnCount = resultTable4.getColumnCount();
			for (int i = 0; i < coloumnCount; i++) {
				String columnName = resultTable4.getColumnName(i);
				if (i == 11) {
					valueString = resultTable4.fetchRow(0)
							.get(11, VoltType.VARBINARY).toString();
					result.put(columnName, new StringByteIterator(valueString));
				} else {
					if (i == 0) {
						valueInt = resultTable4.fetchRow(0)
								.get(0, VoltType.INTEGER).toString();
						result.put(columnName, new StringByteIterator(valueInt));
					} else {
						valueString = resultTable4.fetchRow(0).getString(i);
						result.put(columnName, new StringByteIterator(
								valueString));
					}
				}
			}

		} catch (ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	public int getListOfFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		//System.out.println("getListOfFriends");
		try {
			if (null == client) {
				client = ClientFactory.createClient();
				try {
					client.createConnection("localhost");
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			ClientResponse response = null;
			if (insertImage) {
				try {
					response = client.callProcedure("ListFriendsPic",
							profileOwnerID);
				} catch (NoConnectionsException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				try {
					response = client.callProcedure("ListFriends",
							profileOwnerID);
				} catch (NoConnectionsException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			String valueInt;
			String valueString;
			VoltTable results[] = response.getResults();
			VoltTable resultTable = results[0];
			int coloumnCount = resultTable.getColumnCount();
			for (int i = 0; i < resultTable.getRowCount(); i++) {
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

				for (int j = 0; j < coloumnCount; j++) {
					String columnName = resultTable.getColumnName(j);
					if (j == 13) {
						valueString = resultTable.fetchRow(i)
								.get(j, VoltType.VARBINARY).toString();
						values.put(columnName, new StringByteIterator(
								valueString));
					} else {
						if (j == 0 || j == 1 || j == 2) {
							valueInt = resultTable.fetchRow(i)
									.get(j, VoltType.INTEGER).toString();
							values.put(columnName, new StringByteIterator(
									valueInt));
						} else {
							valueString = resultTable.fetchRow(i).getString(j);
							values.put(columnName, new StringByteIterator(
									valueString));
						}
					}

				}
				result.add(values);
			}

		} catch (ProcCallException e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int viewPendingRequests(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> valuess, boolean insertImage,
			boolean testMode) {
		//System.out.println("viewPendingRequests");
		ClientResponse response;
		try {
			if (null == client) {
				client = ClientFactory.createClient();
				client.createConnection("localhost");
			}
			if (insertImage) {
				response = client.callProcedure("ViewFriendReqPic",
						profileOwnerID);
			} else {
				response = client
						.callProcedure("ViewFriendReq", profileOwnerID);
			}

			String valueInt;
			String valueString;
			VoltTable resultsarray[] = response.getResults();
			VoltTable resultTable = resultsarray[0];
			for (int i = 0; i < resultTable.getRowCount(); i++) {
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				VoltTableRow row = resultTable.fetchRow(i);
				for (int j = 0; j < resultTable.getColumnCount(); j++) {
					if (j == 13) {
						valueString = resultTable.fetchRow(i)
								.get(13, VoltType.VARBINARY).toString();
						values.put(resultTable.getColumnName(j),
								new StringByteIterator(valueString));
					} else {
						if (j == 0 || j == 1 || j == 2) {
							valueInt = row.get(j, VoltType.INTEGER).toString();
							values.put(resultTable.getColumnName(j),
									new StringByteIterator(valueInt));
						} else {
							valueString = row.getString(j);
							values.put(resultTable.getColumnName(j),
									new StringByteIterator(valueString));
						}

					}

				}
			}
		} catch (NoConnectionsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	public int acceptFriendRequest(int invitorID, int inviteeID) {
		//System.out.println("acceptFriendRequest");
		try {
			if (null == client) {
				client = ClientFactory.createClient();
				client.createConnection("localhost");
			}

			client.callProcedure("AcceptFriend", invitorID, inviteeID);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ProcCallException e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int rejectFriendRequest(int invitorID, int inviteeID) {
		//System.out.println("rejectFriendRequest");
		try {
			if (null == client) {
				client = ClientFactory.createClient();
				client.createConnection("localhost");
			}

			client.callProcedure("DeleteFriendship", invitorID, inviteeID);

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ProcCallException e) {
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int inviteFriends(int invitorID, int inviteeID) {
		//System.out.println("inviteFriends");
		try {
			if (null == client) {
				client = ClientFactory.createClient();
				client.createConnection("localhost");
			}
			client.callProcedure("InsertPendingFriends", invitorID, inviteeID,
					1);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int getTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		//System.out.println("getTopKResources");
		try {
			if (null == client) {
				client = ClientFactory.createClient();
				client.createConnection("localhost");
			}

			ClientResponse response;
			response = client.callProcedure("ViewTopKResources",
					profileOwnerID, k + 1);

			VoltTable results[] = response.getResults();
			VoltTable resultTable = results[0];
			for (int i = 0; i < resultTable.getRowCount(); i++) {
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				VoltTableRow row = resultTable.fetchRow(i);
				for (int j = 0; j < resultTable.getColumnCount(); j++) {

					if (j == 0 || j == 1 || j == 2) {
						String valueInt = row.get(j, VoltType.INTEGER)
								.toString();
						values.put(resultTable.getColumnName(j).toLowerCase(),
								new StringByteIterator(valueInt));
					} else {
						String valueString = row.getString(j);
						// System.out.println("\n"+resultTable.getColumnName(j)+":"+valueString);
						values.put(resultTable.getColumnName(j).toLowerCase(),
								new StringByteIterator(valueString));
					}

				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int getResourceComments(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		//System.out.println("getResourceComments");
		try {
			if (null == client) {
				client = ClientFactory.createClient();
				client.createConnection("localhost");
			}

			ClientResponse response;
			response = client
					.callProcedure("ViewCommentOnResource", resourceID);

			VoltTable results[] = response.getResults();
			VoltTable resultTable = results[0];
			for (int i = 0; i < resultTable.getRowCount(); i++) {
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				VoltTableRow row = resultTable.fetchRow(i);
				for (int j = 0; j < resultTable.getColumnCount(); j++) {

					if (j == 0 || j == 1 || j == 2) {
						String valueInt = row.get(j, VoltType.INTEGER)
								.toString();
						values.put(resultTable.getColumnName(j),
								new StringByteIterator(valueInt));
					} else {
						String valueString = row.getString(j);
						values.put(resultTable.getColumnName(j),
								new StringByteIterator(valueString));
					}
				}
				result.add(values);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID) {
		//System.out.println("postCommentOnResource");
		try {
			if (null == client) {
				client = ClientFactory.createClient();
				client.createConnection("localhost");
			}
			ArrayList<String> tempList = new ArrayList<String>();
			HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				String field = entry.getValue().toString();
				tempList.add(field);
			}

			client.callProcedure("PostCommentOnResource", profileOwnerID,
					commentCreatorID, resourceID, tempList.get(1),
					tempList.get(2), tempList.get(0));

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}

	@Override
	public int unFriendFriend(int friendid1, int friendid2) {
		//System.out.println("unFriendFriend");
		try {
			if (null == client) {
				client = ClientFactory.createClient();
				client.createConnection("localhost");
			}

			client.callProcedure("ThawFriendship", friendid1, friendid2,
					friendid2, friendid1);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}
}