package db2DataStore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

import db2DataStore.DB2ClientConstants;

public class DB2Client extends DB implements DB2ClientConstants {

	private static final String DEFAULT_PROP = "";
	private static final String DEFAULT_DRIVER = "com.ibm.db2.jcc.DB2Driver";

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
		String driver = props.getProperty(DRIVER_CLASS, DEFAULT_DRIVER);

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
	public void createSchema(Properties props) {
		String url = props.getProperty(CONNECTION_URL, DEFAULT_PROP);

		Statement stmt = null;
		Statement selectStatement = null;
		try {
			stmt = conn.createStatement();
			selectStatement = conn.createStatement();
			String db = extractDBFromUrl(url);

			String userTableSql = "CREATE TABLE %s.USERS("
					+ "UID int NOT NULL "
					+ "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),"
					+ "UNAME varchar(200)," + "PASSWD varchar(200),"
					+ "FNAME varchar(200)," + "LNAME varchar(200),"
					+ "GNDR varchar(200)," + "DOB varchar(200),"
					+ "JNDATE TIMESTAMP," + "LVDATE TIMESTAMP,"
					+ "ADDR varchar(200)," + "EMAIL varchar(100),"
					+ "TEL VARCHAR(10)," + "NO_PEND_REQ int," + "FRND_CNT int,"
					+ "RSRC_CNT int," + "PRIMARY KEY(UID))";

			dropTable(selectStatement, db, "USERS");
			createTable(stmt, db, "USERS", userTableSql);

			dropTable(selectStatement, db, "FRIENDSHIP");
			createTable(stmt, db, "FRIENDSHIP",
					"CREATE TABLE %s.FRIENDSHIP(INVITERID int, INVITEEID int,STATUS int DEFAULT 1)");

			dropTable(selectStatement, db, "MANIPULATION");
			createTable(
					stmt,
					db,
					"MANIPULATION",
					"CREATE TABLE %s.MANIPULATION(MID int, CREATORID int, RID int, MODIFIERID int, TIMESTAMP VARCHAR(200), TYPE VARCHAR(200), CONTENT VARCHAR(200))");

			dropTable(selectStatement, db, "RESOURCE");

			createTable(
					stmt,
					db,
					"RESOURCE",
					"CREATE TABLE %s.RESOURCE(RID int,CREATORID int,WALLUSERID int, TYPE VARCHAR(200),BODY VARCHAR(200), DOC VARCHAR(200))");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stmt != null)
				try {
					stmt.close();
					conn.commit();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
		}
	}

	private String extractDBFromUrl(String url) {
		String[] parts = url.split("/");
		int n = parts.length;
		String db = parts[n - 1];
		return db;
	}

	private void createTable(Statement stmt, String db, String table, String sql) {
		try {
			stmt.executeUpdate(String.format(sql, db));
			System.out.println(String.format("%s.%s CREATED", db, table));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void dropTable(Statement stmt, String db, String table) {
		String query = String.format("DROP TABLE %s.%s", db.toUpperCase(),
				table);
		try {
			stmt.executeUpdate(query);
			System.out.println(String.format("DROPPED TABLE %s.%s", db, table));
		} catch (SQLException e) {
			System.out.println(String.format("%s.%s Table does not exist", db,
					table));
		}
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
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
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage,
			int imageSize) {

		String fields = "(";
		String sqlValues = "(";
		int fieldCounter = 0;
		HashMap<String, String> usersMaps = new HashMap<String, String>();
		usersMaps.put("username", "uname");
		usersMaps.put("pw", "passwd");
		usersMaps.put("gender", "gndr");
		usersMaps.put("jdate", "jndate");
		usersMaps.put("ldate", "lvdate");
		usersMaps.put("address", "addr");
		usersMaps.put("tel", "tel");

		for (Entry<String, ByteIterator> it : values.entrySet()) {
			if (0 == fieldCounter) {
				fields += getKey(it.getKey(), usersMaps);
				sqlValues += "?";
			} else {
				fields += "," + getKey(it.getKey(), usersMaps);
				sqlValues += ",?";
			}
			fieldCounter++;
		}
		fields += ",NO_PEND_REQ,FRND_CNT,RSRC_CNT)";
		sqlValues += ",?,?,?)";
		String query = "INSERT into test.USERS " + fields + " VALUES "
				+ sqlValues;

		// System.out.println(query);

		PreparedStatement preparedStatement;
		try {
			preparedStatement = conn.prepareStatement(query);
			int cnt = 1;
			for (Entry<String, ByteIterator> entry : values.entrySet()) {
				String key = entry.getKey();
				setValue(key, usersMaps, preparedStatement, cnt, entry
						.getValue().toString());
				cnt++;
			}
			for (int i = 0; i < 3; i++) {
				preparedStatement.setInt(cnt++, 0);
			}

			// System.out.println(cnt + " ");
			int rs = preparedStatement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return 0;
	}

	private void setValue(String key, HashMap<String, String> usersMaps,
			PreparedStatement preparedStatement, int cnt, String v)
			throws SQLException {
		if (!usersMaps.containsKey(key)) {
			String r = v.substring(0, Math.min(199, v.length()));
			preparedStatement.setString(cnt, r);
			// System.out.println(key);
			// System.out.println(r);
		} else {
			key = usersMaps.get(key);
			if ("tel".equals(key)) {
				String r = v.substring(0, Math.min(9, v.length()));
				preparedStatement.setString(cnt, r);
				// System.out.println(key);
				// System.out.println(r);
			} else if ("jndate".equals(key)) {
				java.sql.Timestamp r = new java.sql.Timestamp(
						new java.util.Date().getTime());
				preparedStatement.setTimestamp(cnt, r);
				// System.out.println(key);
				// System.out.println(r);
			} else if ("lvdate".equals(key)) {
				preparedStatement.setTimestamp(cnt, null);
				// System.out.println(key);
				// System.out.println("NULL");
			} else {
				String r = v.substring(0, Math.min(199, v.length()));
				preparedStatement.setString(cnt, r);
				// System.out.println(key);
				// System.out.println(r);
			}
		}
	}

	private String getValue(String key, HashMap<String, String> usersMaps) {
		if (!usersMaps.containsKey(key)) {
			return "'yo'";
		}
		key = usersMaps.get(key);
		if ("jndate".equals(key)) {
			return "current timestamp";
		}
		if ("lvdate".equals(key)) {
			return "NULL";
		}
		return "'yo'";
	}

	private String getKey(String key, HashMap<String, String> usersMaps) {
		if (usersMaps.containsKey(key)) {
			return usersMaps.get(key);
		}
		return key;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID, HashMap<String, ByteIterator> values) {
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
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		// TODO Auto-generated method stub
		return new HashMap<String, String>();
	}

}