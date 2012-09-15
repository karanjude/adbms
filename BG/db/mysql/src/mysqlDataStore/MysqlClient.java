package mysqlDataStore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import com.yahoo.ycsb.ByteIterator;
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
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values, boolean insertImage,
			int imageSize) {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int CreateFriendship(int memberA, int memberB) {
		// TODO Auto-generated method stub
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

			stmt.executeUpdate("DROP TABLE IF EXISTS MANIPULATION");

			stmt.executeUpdate("CREATE TABLE MANIPULATION(MID int, CREATORID int, RID int, MODIFIERID int, TIMESTAMP VARCHAR(200), TYPE VARCHAR(200), CONTENT VARCHAR(200));");

			System.out.println("TABLE MANIPULATION CREATED");

			stmt.executeUpdate("DROP TABLE IF EXISTS RESOURCES");

			stmt.executeUpdate("CREATE TABLE RESOURCES(RID int,CREATORID int,WALLUSERID int, TYPE VARCHAR(200),BODY VARCHAR(200), DOC VARCHAR(200));");
		
			System.out.println("TABLE RESOURCES CREATED");
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