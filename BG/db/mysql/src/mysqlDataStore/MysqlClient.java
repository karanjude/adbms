package mysqlDataStore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class MysqlClient extends DB implements MysqlClientConstants {

	private static final String DEFAULT_PROP = "";

	private boolean initialized;
	private Properties props;
	private Connection conn;

	private ConcurrentHashMap<Integer, PreparedStatement> newCachedStatements;

	@Override
	public void init() throws DBException {
		if (initialized) {
			System.out.println("Client connection already initialized.");
			return;
		}
		props = getProperties();
		String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
		String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
		String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
		String driver = props.getProperty(DRIVER_CLASS);

		try {
			if (driver != null) {
				Class.forName(driver);
			}
			for (String url : urls.split(",")) {
				conn = DriverManager.getConnection(url, user, passwd);
				conn.setAutoCommit(true);
			}
			newCachedStatements = new ConcurrentHashMap<Integer, PreparedStatement>();
		} catch (ClassNotFoundException e) {
			System.out.println("Error in initializing the JDBS driver: " + e);
			throw new DBException(e);
		} catch (SQLException e) {
			System.out.println("Error in database operation: " + e);
			throw new DBException(e);
		} catch (NumberFormatException e) {
			System.out.println("Invalid value for fieldcount property. " + e);
			throw new DBException(e);
		}
		initialized = true;
	}

	@Override
	public void cleanup(boolean warmup) throws DBException {
		try {
			// close all cached prepare statements
			Set<Integer> statementTypes = newCachedStatements.keySet();
			Iterator<Integer> it = statementTypes.iterator();
			while (it.hasNext()) {
				int stmtType = it.next();
				if (newCachedStatements.get(stmtType) != null)
					newCachedStatements.get(stmtType).close();
			}
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	@Override
	public int insert(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage,
			int imageSize) {
		if (entitySet.equals("users")) {
			ArrayList<String> tempList = new ArrayList<String>();
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				String field = entry.getValue().toString();
				tempList.add(field);
			}

			InsertUserProc insertProc = new InsertUserProc(conn);
			insertProc
					.execute(tempList.get(0), tempList.get(1), tempList.get(2),
							tempList.get(3), tempList.get(4), tempList.get(5),
							tempList.get(6), tempList.get(7), tempList.get(8),
							tempList.get(9), tempList.get(10), 0, 0, 0);

		} else if (entitySet.equals("resources")) {

			ArrayList<String> tempList = new ArrayList<String>();
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				String field = entry.getValue().toString();
				tempList.add(field);
			}

			InsertResourcesProc insertResourceProc = new InsertResourcesProc(
					conn);

			insertResourceProc.execute(tempList.get(0), tempList.get(1),
					tempList.get(2), tempList.get(3), tempList.get(4));

		}
		return 0;
	}

	@Override
	public int getUserProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getListOfFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int viewPendingRequests(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> values, boolean insertImage,
			boolean testMode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int acceptFriendRequest(int invitorID, int inviteeID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int rejectFriendRequest(int invitorID, int inviteeID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int inviteFriends(int invitorID, int inviteeID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getResourceComments(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int unFriendFriend(int friendid1, int friendid2) {
		ThawFriendshipProc proc = new ThawFriendshipProc(conn);
		proc.execute(friendid1, friendid2, friendid2, friendid1);
		return 0;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		HashMap<String, String> stats = new HashMap<String, String>();

		SelectUserCountProc selectUserCountProc = new SelectUserCountProc(conn);
		Integer result = selectUserCountProc.execute();
		stats.put("usercount", result.toString());

		UserOffsetProc userOffsetProc = new UserOffsetProc(conn);
		Integer offset = userOffsetProc.execute();

		ResourceCountProc resourceCountproc = new ResourceCountProc(conn);
		stats.put("resourcesperuser", resourceCountproc.execute(offset)
				.toString());

		FriendsPerUserProc friendsPerUserProc = new FriendsPerUserProc(conn);
		stats.put("avgfriendsperuser", friendsPerUserProc.execute(offset)
				.toString());

		PendingFriendsPerUserProc pendingFriendsPerUserProc = new PendingFriendsPerUserProc(
				conn);
		stats.put("avgpendingperuser", pendingFriendsPerUserProc
				.execute(offset).toString());

		return stats;
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		InsertFriendsProc proc = new InsertFriendsProc(conn);
		proc.execute(friendid1, friendid2, 2);
		return 0;
	}

	@Override
	public void createSchema(Properties props) {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();

			stmt.executeUpdate("DROP TABLE IF EXISTS FRIENDSHIP");

			stmt.executeUpdate("CREATE TABLE FRIENDSHIP(INVITERID int, INVITEEID int,STATUS int DEFAULT 1)");

			System.out.println("TABLE FRIENDSHIP CREATED");

			stmt.executeUpdate("DROP TABLE IF EXISTS MODIFY");

			stmt.executeUpdate("CREATE TABLE MODIFY(CREATORID int, RID int, MODIFIERID int, TIMESTAMP VARCHAR(255), TYPE VARCHAR(255), CONTENT VARCHAR(255));");

			stmt.executeUpdate("CREATE INDEX i4 ON Modify (rid);");

			System.out.println("TABLE MODIFY CREATED");

			stmt.executeUpdate("DROP TABLE IF EXISTS RESOURCE");

			stmt.executeUpdate("CREATE TABLE Resource ("
					+ "  rid INTEGER NOT NULL AUTO_INCREMENT,"
					+ "  creatorid INTEGER," + "  wallUserId INTEGER NOT NULL,"
					+ "  type VARCHAR(255)," + "  body VARCHAR(255),"
					+ "  doc VARCHAR(255)," + "  PRIMARY KEY (rid)" + "); ");

			stmt.executeUpdate("CREATE INDEX i3 ON Resource (wallUserId);");

			System.out.println("TABLE RESOURCE CREATED");

			stmt.executeUpdate("DROP TABLE IF EXISTS Users");

			stmt.executeUpdate("CREATE TABLE Users ("
					+ "  userid INTEGER NOT NULL AUTO_INCREMENT,"
					+ "  username VARCHAR(255)," + "  pw VARCHAR(255),"
					+ "  fname VARCHAR(255)," + "  lname VARCHAR(255),"
					+ "  gender VARCHAR(255)," + "  dob VARCHAR(255),"
					+ "  jdate VARCHAR(255)," + "  ldate VARCHAR(255),"
					+ "  address VARCHAR(255)," + "  email VARCHAR(255),"
					+ "  tel VARCHAR(255)," + "  confirmedFriends INTEGER,"
					+ "  pendingFriends INTEGER," + "  resourceCount INTEGER,"
					+ "  PRIMARY KEY (userid)" + "); ");

			stmt.executeUpdate("CREATE INDEX i2 ON Users (confirmedFriends,pendingFriends,resourceCount);");

			System.out.println("TABLE Users CREATED");

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
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

}