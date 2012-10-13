package db2DataStore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

public class DB2Client extends DB implements DB2ClientConstants {

	private static final String DEFAULT_PROP = "";
	private static final String DEFAULT_DRIVER = "com.ibm.db2.jcc.DB2Driver";

	private boolean initialized;
	private Properties props;
	private Connection conn;
	private Connection transactionlConnection;

	private ConcurrentHashMap<Integer, PreparedStatement> newCachedStatements;
	private int count = 0;
	private String dbname;

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
		String url = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
		dbname = extractDBFromUrl(url);
		try {
			if (driver != null) {
				Class.forName(driver);
			}
			for (String url_ : urls.split(",")) {
				conn = DriverManager.getConnection(url_, user, passwd);
				conn.setAutoCommit(true);
				transactionlConnection = DriverManager.getConnection(url_,
						user, passwd);
				transactionlConnection.setAutoCommit(true);
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
			if (null != transactionlConnection) {
				transactionlConnection.close();
			}
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
					+ "GENERATED ALWAYS AS IDENTITY (START WITH 0, INCREMENT BY 1),"
					+ "UNAME varchar(200)," + "PASSWD varchar(200),"
					+ "FNAME varchar(200)," + "LNAME varchar(200),"
					+ "GNDR varchar(200)," + "DOB varchar(200),"
					+ "JNDATE TIMESTAMP," + "LVDATE TIMESTAMP,"
					+ "ADDR varchar(200)," + "EMAIL varchar(100),"
					+ "TEL VARCHAR(10)," + "NO_PEND_REQ int DEFAULT 0,"
					+ "NO_REQ_REJECT int DEFAULT 0,"
					+ "FRND_CNT int DEFAULT 0," + "RSRC_CNT int,"
					+ "PRIMARY KEY(UID))";

			dropTable(selectStatement, db, "USERS");
			createTable(stmt, db, "USERS", userTableSql);

			dropTable(selectStatement, db, "PENDING_FRIENDSHIP");
			createTable(
					stmt,
					db,
					"PENDING_FRIENDSHIP",
					"CREATE TABLE %s.PENDING_FRIENDSHIP(USERID1 int NOT NULL, USERID2 int NOT NULL,PRIMARY KEY(USERID1, USERID2))");

			dropTable(selectStatement, db, "FRIENDSHIP");
			createTable(
					stmt,
					db,
					"FRIENDSHIP",
					"CREATE TABLE %s.FRIENDSHIP(USERID1 int NOT NULL, USERID2 int NOT NULL,PRIMARY KEY(USERID1, USERID2))");

			dropTable(selectStatement, db, "MANIPULATION");
			createTable(
					stmt,
					db,
					"MANIPULATION",
					"CREATE TABLE %s.MANIPULATION(MID int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 0, INCREMENT BY 1), CREATORID int, RID int, MODIFIERID int, MTIME TIMESTAMP, TYPE CHAR(1), CONTENT VARCHAR(200))");

			dropTable(selectStatement, db, "RESOURCES");

			String resourceCreateQuery = "CREATE TABLE %s.RESOURCES("
					+ "RID int NOT NULL "
					+ "GENERATED ALWAYS AS IDENTITY (START WITH 0, INCREMENT BY 1),"
					+ "CREATORID int NOT NULL,WALLUSERID int NOT NULL,"
					+ "TYPE VARCHAR(200)," + "BODY VARCHAR(200),"
					+ "DOC VARCHAR(200),"
					+ "PRIMARY KEY(RID,CREATORID,WALLUSERID))";

			createTable(stmt, db, "RESOURCES", resourceCreateQuery);
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
			System.out.println(String.format(sql, db));
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
		System.out.println("create friendship " + friendid1 + " " + friendid2);

		// update USERS set FRND_CNT = (select FRND_CNT + 1 from USERS where
		// UID
		// = A) where UID

		String updateFriendCountSql = String
				.format(
						"update %s.USERS "
								+ "set FRND_CNT = (select FRND_CNT + 1 from %s.Users where UID = ?), "
								+ "NO_PEND_REQ = (select NO_PEND_REQ - ? from %s.Users where UID = ?) "
								+ "where UID=?", dbname, dbname, dbname);

		String insertFriendshipQuery = String.format(
				"Insert into %s.FRIENDSHIP(USERID1, USERID2) VALUES(?,?)",
				dbname);

		String deletePendingFriendshipQuery1 = String
				.format(
						"delete from %s.PENDING_FRIENDSHIP where userid1 = ? and userid2 = ?",
						dbname);

		HashMap<Integer, Integer> deletedRecordCount = new HashMap<Integer, Integer>();

		ArrayList<PreparedStatement> statements = new ArrayList<PreparedStatement>();
		try {
			deletePendingFriendshipRequests(friendid1, friendid2,
					deletePendingFriendshipQuery1, deletedRecordCount,
					statements);

			deletePendingFriendshipRequests(friendid2, friendid1,
					deletePendingFriendshipQuery1, deletedRecordCount,
					statements);

			insertFriendshipRecord(friendid1, friendid2, insertFriendshipQuery,
					statements);

			updateFriendShipRecord(friendid1, friendid2, updateFriendCountSql,
					statements, deletedRecordCount);
			transactionlConnection.commit();
		} catch (Exception e) {
			try {
				transactionlConnection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			return 1;
		} finally {
			for (PreparedStatement preparedStatement : statements) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return 0;
	}

	private void deletePendingFriendshipRequests(int friendid1, int friendid2,
			String deletePendingFriendshipQuery1,
			HashMap<Integer, Integer> deletedRecordCount,
			ArrayList<PreparedStatement> statements) throws SQLException {
		PreparedStatement preparedStatement;
		preparedStatement = transactionlConnection
				.prepareStatement(deletePendingFriendshipQuery1);
		preparedStatement.setInt(1, friendid1);
		preparedStatement.setInt(2, friendid2);
		int r = preparedStatement.executeUpdate();
		deletedRecordCount.put(friendid1, r);
		statements.add(preparedStatement);
	}

	private void updateFriendShipRecord(int friendid1, int friendid2,
			String updateFriendCountSql,
			ArrayList<PreparedStatement> statements,
			HashMap<Integer, Integer> deletedRecordCount) throws SQLException {
		statements.add(updateFriendCount(friendid1, updateFriendCountSql,
				deletedRecordCount.get(friendid1)));
		statements.add(updateFriendCount(friendid2, updateFriendCountSql,
				deletedRecordCount.get(friendid2)));
	}

	private void updateFriendShipRecord(int friendid1, int friendid2,
			String updateFriendCountSql, ArrayList<PreparedStatement> statements)
			throws SQLException {
		statements.add(updateFriendCount(friendid1, updateFriendCountSql));
		statements.add(updateFriendCount(friendid2, updateFriendCountSql));
	}

	private void insertFriendshipRecord(int friendid1, int friendid2,
			String query, ArrayList<PreparedStatement> statements)
			throws SQLException {
		statements.add(insertFriends(friendid1, friendid2, query));
		statements.add(insertFriends(friendid2, friendid1, query));
	}

	private PreparedStatement updateFriendCount(int friendid, String query,
			Integer count) throws SQLException {
		PreparedStatement preparedStatement;
		preparedStatement = transactionlConnection.prepareStatement(query);
		preparedStatement.setInt(1, friendid);
		preparedStatement.setInt(2, count);
		preparedStatement.setInt(3, friendid);
		preparedStatement.setInt(4, friendid);
		preparedStatement.executeUpdate();
		return preparedStatement;
	}

	private PreparedStatement updateFriendCount(int friendid, String query)
			throws SQLException {
		PreparedStatement preparedStatement;
		preparedStatement = transactionlConnection.prepareStatement(query);
		preparedStatement.setInt(1, friendid);
		preparedStatement.setInt(2, friendid);
		preparedStatement.executeUpdate();
		return preparedStatement;
	}

	private PreparedStatement insertFriends(int friendid1, int friendid2,
			String query) throws SQLException {
		PreparedStatement preparedStatement;
		preparedStatement = transactionlConnection.prepareStatement(query);
		preparedStatement.setInt(1, friendid1);
		preparedStatement.setInt(2, friendid2);
		preparedStatement.executeUpdate();
		return preparedStatement;
	}

	@Override
	public int acceptFriend(int friendid1, int friendid2) {
		return CreateFriendship(friendid1, friendid2);
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		return 0;
	}

	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage,
			int imageSize) {
		// System.out.println(entitySet);

		if (entitySet.equals("users")) {
			insertUsers(values, dbname);
		} else if (entitySet.equals("resources")) {
			insertResources(values, dbname);
		}

		return 0;
	}

	private void insertResources(HashMap<String, ByteIterator> values, String db) {
		System.out.println("inserting resources " + count++);
		String query = insertResourceQuery(values, db);
		// System.out.println(query);

		Integer creatorId = Integer.parseInt(executeInsertResourceQuery(values,
				query));

		String updateFriendCountSql = String
				.format(
						"update %s.USERS set RSRC_CNT = (select RSRC_CNT + 1 from %s.Users where UID = ?) where UID=?",
						dbname, dbname);

		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = transactionlConnection
					.prepareStatement(updateFriendCountSql);
			preparedStatement.setInt(1, creatorId);
			preparedStatement.setInt(2, creatorId);
			preparedStatement.executeUpdate();
			transactionlConnection.commit();
		} catch (SQLException e) {
			try {
				transactionlConnection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		} finally {
			if (null != preparedStatement) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private String executeInsertResourceQuery(
			HashMap<String, ByteIterator> values, String query) {
		String creatorid = null;
		PreparedStatement preparedStatement;

		try {
			preparedStatement = conn.prepareStatement(query);
			int cnt = 1;
			for (Entry<String, ByteIterator> entry : values.entrySet()) {
				String key = entry.getKey();
				String v = entry.getValue().toString();
				if (key.equals("creatorid") || key.equals("walluserid")) {
					if (key.equals("creatorid")) {
						creatorid = v;
					}
					preparedStatement.setInt(cnt, Integer.parseInt(v));
				} else {
					String r = v.substring(0, Math.min(190, v.length()));
					preparedStatement.setString(cnt, r);
				}
				cnt++;
			}
			int rs = preparedStatement.executeUpdate();
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return creatorid;
	}

	private String insertResourceQuery(HashMap<String, ByteIterator> values,
			String db) {
		String fields = "(";
		String sqlValues = "(";
		int fieldCounter = 0;

		for (Entry<String, ByteIterator> it : values.entrySet()) {
			if (0 == fieldCounter) {
				fields += it.getKey();
				sqlValues += "?";
			} else {
				fields += "," + it.getKey();
				sqlValues += ",?";
			}
			fieldCounter++;
		}
		fields += ")";
		sqlValues += ")";
		String query = "INSERT into %s.RESOURCES " + fields + " VALUES "
				+ sqlValues;
		query = String.format(query, db);
		return query;
	}

	private void insertUsers(HashMap<String, ByteIterator> values, String db) {
		System.out.println("inserting users");
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
		String query = "INSERT into %s.USERS " + fields + " VALUES "
				+ sqlValues;

		query = String.format(query, db);

		System.out.println(query);

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
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
			} else if ("FRND_CNT".equals(key) || "NO_PEND_REQ".equals(key)
					|| "RSRC_CNT".equals(key)) {
				preparedStatement.setInt(cnt, 0);
			} else {
				String r = v.substring(0, Math.min(199, v.length()));
				preparedStatement.setString(cnt, r);
				// System.out.println(key);
				// System.out.println(r);
			}
		}
	}

	private String getKey(String key, HashMap<String, String> usersMaps) {
		if (usersMaps.containsKey(key)) {
			return usersMaps.get(key);
		}
		return key;
	}

	@Override
	public int inviteFriend(int friendid1, int friendid2) {
		System.out.println("create pending friendship " + friendid1 + " "
				+ friendid2);

		String updateFriendCountSql = String
				.format(
						"update %s.USERS set "
								+ "NO_PEND_REQ = (select NO_PEND_REQ + 1 from %s.Users where UID = ?) "
								+ "where UID=?", dbname, dbname);

		String insertFriendshipQuery = String
				.format(
						"Insert into %s.PENDING_FRIENDSHIP(USERID1, USERID2) VALUES(?,?)",
						dbname);

		ArrayList<PreparedStatement> statements = new ArrayList<PreparedStatement>();
		try {
			insertFriendshipRecord(friendid1, friendid2, insertFriendshipQuery,
					statements);

			updateFriendShipRecord(friendid1, friendid2, updateFriendCountSql,
					statements);

			transactionlConnection.commit();
		} catch (Exception e) {
			try {
				transactionlConnection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			return 1;
		} finally {
			for (PreparedStatement preparedStatement : statements) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return 0;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {

		String query = String
				.format(
						"select u.* from %s.users u, %s.friendship f where f.userid1 = ? and f.userid2 = u.uid",
						dbname, dbname);

		PreparedStatement statement;
		ResultSet rs = null;
		boolean error = false;
		try {
			statement = conn.prepareStatement(query);
			statement.setInt(1, profileOwnerID);
			rs = statement.executeQuery();
			int r = populate(result, rs);
			if (r > 0)
				error = true;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != rs)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (error)
			return 1;
		return 0;
	}

	private int populate(Vector<HashMap<String, ByteIterator>> result,
			ResultSet rs) {
		ResultSetMetaData md;
		try {
			md = rs.getMetaData();
			int col = md.getColumnCount();
			while (rs.next()) {
				HashMap<String, ByteIterator> map = new HashMap<String, ByteIterator>();
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					map.put(col_name.toLowerCase(), new StringByteIterator(
							value));
				}
				result.add(map);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID, HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		// "CREATE TABLE %s.MANIPULATION(MID int NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 0, INCREMENT BY 1), CREATORID int, RID int, MODIFIERID int, MTIME TIMESTAMP, TYPE CHAR(1), CONTENT VARCHAR(200))");

		String insertQuery = String
				.format(
						"insert into %s.MANIPULATION(CREATORID,RID,MODIFIERID,MTIME,TYPE,CONTENT) VALUES(?,?,?,CURRENT TIMESTAMP,'C',?)",
						dbname);
		try {
			PreparedStatement p = conn.prepareStatement(insertQuery);
			p.setInt(1, profileOwnerID);
			p.setInt(2, resourceID);
			p.setInt(3, commentCreatorID);
			String comment = values.get("content").toString();
			if (comment.length() == 0)
				comment = "test comment";
			else
				comment = comment.substring(0, Math.min(comment.length(), 200));
			p.setString(4, comment);
			p.executeUpdate();
			p.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return 1;
		}
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
	public int rejectFriend(int friendid1, int friendid2) {
		System.out.println("reject pending friendship " + friendid1 + " "
				+ friendid2);

		String decreasePendingRequestsSql = String
				.format(
						"update %s.USERS set "
								+ "NO_PEND_REQ = (select NO_PEND_REQ - 1 from %s.Users where UID = ?) "
								+ "where UID=?", dbname, dbname);

		String increaseNumberOfRejectsSql = String
				.format(
						"update %s.USERS set "
								+ "NO_PEND_REQ = (select NO_PEN_REQ - 1 from %s.Users where UID = ?), "
								+ "NO_REQ_REJECT = (select NO_REQ_REJECT + 1 from %s.Users where UID = ?) "
								+ "where UID=?", dbname, dbname, dbname);

		String deletePendingFriendshipQuery = String
				.format(
						"delete from %s.PENDING_FRIENDSHIP(USERID1, USERID2) where userid1=? and userid2=?",
						dbname);

		ArrayList<PreparedStatement> statements = new ArrayList<PreparedStatement>();
		try {
			statements.add(deletePendingFriendship(friendid1, friendid2,
					deletePendingFriendshipQuery));
			statements.add(deletePendingFriendship(friendid2, friendid1,
					deletePendingFriendshipQuery));

			statements.add(decreasePendingFriendshipCount(friendid1,
					decreasePendingRequestsSql));

			statements.add(increaseRejectionCount(friendid2,
					increaseNumberOfRejectsSql));

			transactionlConnection.commit();
		} catch (Exception e) {
			try {
				transactionlConnection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			return 1;
		} finally {
			for (PreparedStatement preparedStatement : statements) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return 0;
	}

	private PreparedStatement increaseRejectionCount(int friendid2,
			String increaseNumberOfRejectsSql) throws SQLException {
		PreparedStatement preparedStatement;
		preparedStatement = transactionlConnection
				.prepareStatement(increaseNumberOfRejectsSql);
		preparedStatement.setInt(1, friendid2);
		preparedStatement.setInt(2, friendid2);
		preparedStatement.setInt(3, friendid2);
		preparedStatement.executeUpdate();
		return preparedStatement;
	}

	private PreparedStatement decreasePendingFriendshipCount(int friendid1,
			String decreasePendingRequestsSql) throws SQLException {
		PreparedStatement preparedStatement;
		preparedStatement = transactionlConnection
				.prepareStatement(decreasePendingRequestsSql);
		preparedStatement.setInt(1, friendid1);
		preparedStatement.setInt(2, friendid1);
		preparedStatement.executeUpdate();
		return preparedStatement;
	}

	private PreparedStatement deletePendingFriendship(int friendid1,
			int friendid2, String deletePendingFriendshipQuery)
			throws SQLException {
		PreparedStatement preparedStatement;
		preparedStatement = transactionlConnection
				.prepareStatement(deletePendingFriendshipQuery);
		preparedStatement.setInt(1, friendid1);
		preparedStatement.setInt(2, friendid2);
		preparedStatement.executeUpdate();
		return preparedStatement;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		String deleteQuery = String.format(
				"delete from %s.FRIENDSHIP where userid1 = ? and userid2 = ?",
				dbname);

		String updateFriendCountSql = String
				.format(
						"update %s.USERS "
								+ "set FRND_CNT = (select FRND_CNT - 1 from %s.Users where UID = ?) "
								+ "where UID=?", dbname, dbname);

		List<Statement> statements = new ArrayList<Statement>();
		try {
			deleteFriends(statements, deleteQuery, friendid1, friendid2);
			deleteFriends(statements, deleteQuery, friendid2, friendid1);
			updateFriendCount(statements, updateFriendCountSql, friendid1);
			updateFriendCount(statements, updateFriendCountSql, friendid2);
			transactionlConnection.commit();
		} catch (SQLException e) {
			try {
				transactionlConnection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
			return 1;
		} finally {
			for (Statement statement : statements) {
				try {
					statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		return 0;
	}

	private void updateFriendCount(List<Statement> statements,
			String updateFriendCountSql, int friendid1) throws SQLException {
		PreparedStatement statement = transactionlConnection
				.prepareStatement(updateFriendCountSql);
		statement.setInt(1, friendid1);
		statement.setInt(2, friendid1);
		statement.executeUpdate();
		statements.add(statement);
	}

	private void deleteFriends(List<Statement> statements, String deleteQuery,
			int friendid1, int friendid2) throws SQLException {
		PreparedStatement statement = transactionlConnection
				.prepareStatement(deleteQuery);
		statement.setInt(1, friendid1);
		statement.setInt(2, friendid2);
		statement.executeUpdate();
		statements.add(statement);
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {

		String query = String.format(
				"select * from %s.Manipulation where rid = ? and type = 'C'",
				dbname);

		try {
			PreparedStatement statement = conn.prepareStatement(query);
			statement.setInt(1, resourceID);
			ResultSet rs = statement.executeQuery();
			int v = populateTopKUserRecord(result, rs);
		} catch (SQLException e) {
			e.printStackTrace();
			return 1;
		}

		return 0;
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage,
			boolean testMode) {

		String query = String
				.format(
						"select u.* from %s.users u, %s.PENDING_FRIENDSHIP f where f.userid1 = ? and f.userid2 = u.uid",
						dbname, dbname);

		PreparedStatement statement;
		ResultSet rs = null;
		boolean error = false;
		try {
			statement = conn.prepareStatement(query);
			statement.setInt(1, profileOwnerID);
			rs = statement.executeQuery();
			int r = populate(result, rs);
			if (r > 0)
				error = true;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != rs)
					rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (error)
			return 1;
		return 0;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		// "friendcount", "resourcecount" and "pendingcount"
		String query = String
				.format(
						"select FRND_CNT, RSRC_CNT, NO_PEND_REQ from %s.users where uid = ?",
						dbname);

		try {
			PreparedStatement statement = conn.prepareStatement(query);
			statement.setInt(1, profileOwnerID);
			ResultSet rs = statement.executeQuery();
			int v = populateUserRecord(result, rs);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		if (requesterID != profileOwnerID) {
			if (result.containsKey("pendingcount"))
				result.remove("pendingcount");
		}
		return 0;
	}

	private int populateUserRecord(HashMap<String, ByteIterator> result,
			ResultSet rs) {
		ResultSetMetaData md;
		try {
			md = rs.getMetaData();
			int col = md.getColumnCount();
			while (rs.next()) {
				// "friendcount", "resourcecount" and "pendingcount"
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					if (col_name.equals("FRND_CNT")) {
						result
								.put("friendcount", new StringByteIterator(
										value));
					}
					if (col_name.equals("NO_PEND_REQ")) {
						result.put("pendingcount",
								new StringByteIterator(value));
					}
					if (col_name.equals("RSRC_CNT")) {
						result.put("resourcecount", new StringByteIterator(
								value));
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return 1;
		}

		return 0;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		String query = String
				.format(
						"select * from %s.resources where ( creatorid = %s or walluserid = %s ) order by RID DESC FETCH FIRST %s ROWS ONLY",
						dbname, profileOwnerID, profileOwnerID, k);

		try {
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			int v = populateTopKUserRecord(result, rs);
		} catch (SQLException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	private int populateTopKUserRecord(
			Vector<HashMap<String, ByteIterator>> result, ResultSet rs) {
		ResultSetMetaData md;
		try {
			md = rs.getMetaData();
			int col = md.getColumnCount();
			while (rs.next()) {
				HashMap<String, ByteIterator> rMap = new HashMap<String, ByteIterator>();
				// "friendcount", "resourcecount" and "pendingcount"
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					rMap.put(col_name.toLowerCase(), new StringByteIterator(
							value));
				}
				result.add(rMap);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		HashMap<String, String> stats = new HashMap<String, String>();
		Statement st = null;
		String query = "";
		try {
			st = conn.createStatement();
			// get user count
			getUserCount(stats, st);
			// get user offset
			String offset = getUserOffset(st);
			// get resources per user
			getResourceCountPerUser(stats, st, offset);
			// get number of friends per user
			getFriendCountPerUser(stats, st, offset);
			getPendingFriendRequestsPerUser(stats, st, offset);
		} catch (SQLException sx) {
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
		return stats;
	}

	private void getPendingFriendRequestsPerUser(HashMap<String, String> stats,
			Statement st, String offset) throws SQLException {
		String query;
		query = String.format("select NO_PEND_REQ from %s.users where uid=%s",
				dbname, Integer.parseInt(offset));
		ResultSet r2 = st.executeQuery(query);
		if (r2.next()) {
			stats.put("avgpendingperuser", r2.getString(1));
		} else
			stats.put("avgpendingperuser", "0");
		if (r2 != null)
			r2.close();
	}

	private void getFriendCountPerUser(HashMap<String, String> stats,
			Statement st, String offset) throws SQLException {
		String query;
		query = String.format("select FRND_CNT from %s.users where uid=%s",
				dbname, Integer.parseInt(offset));
		ResultSet r2 = st.executeQuery(query);
		if (r2.next()) {
			stats.put("avgfriendsperuser", r2.getString(1));
		} else
			stats.put("avgfriendsperuser", "0");
		if (r2 != null)
			r2.close();
	}

	private void getResourceCountPerUser(HashMap<String, String> stats,
			Statement st, String offset) throws SQLException {
		String query;
		query = String.format("SELECT RSRC_CNT from %s.users where uid=%s",
				dbname, Integer.parseInt(offset));
		ResultSet r2 = st.executeQuery(query);
		if (r2.next()) {
			stats.put("resourcesperuser", r2.getString(1));
		} else {
			stats.put("resourcesperuser", "0");
		}
		if (r2 != null)
			r2.close();
	}

	private String getUserOffset(Statement st) throws SQLException {
		String query;
		query = String.format("SELECT min(uid) from %s.users", dbname);
		ResultSet r2 = st.executeQuery(query);
		String offset = "0";
		if (r2.next()) {
			offset = r2.getString(1);
		}
		return offset;
	}

	private ResultSet getUserCount(HashMap<String, String> stats, Statement st)
			throws SQLException {
		ResultSet rs;
		String query;
		query = String.format("SELECT count(*) from %s.users", dbname);
		rs = st.executeQuery(query);
		if (rs.next()) {
			stats.put("usercount", rs.getString(1));
		} else
			stats.put("usercount", "0"); // sth is wrong - schema is missing
		if (rs != null)
			rs.close();
		return rs;
	}
}