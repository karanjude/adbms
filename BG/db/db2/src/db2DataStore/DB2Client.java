package db2DataStore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Blob;
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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

import db2DataStore.DB2ClientConstants;

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
	private String imagepath;

	@Override
	public void init() throws DBException {
		System.out.println("Init started");
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
		imagepath = props.getProperty(Client.IMAGE_PATH_PROPERTY,
				Client.IMAGE_PATH_PROPERTY_DEFAULT);

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
		System.out.println("Init Done");
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
		System.out.println("create schema called");
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
					+ "FRND_CNT int DEFAULT 0," + "RSRC_CNT int," + "PIC BLOB,"
					+ "TPIC BLOB," + "PRIMARY KEY(UID))";

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

			String createFriendshipProcedure = "CREATE PROCEDURE %s.CreateFriendship (IN USERID1 int, IN USERID2 int) "
					+ "BEGIN "
					+ " DELETE from %s.PENDING_FRIENDSHIP where USERID1 = USERID1 and USERID2 = USERID2;"
					+ " DELETE from %s.PENDING_FRIENDSHIP where USERID1 = USERID2 and USERID2 = USERID1;"
					+ "	INSERT INTO %s.FRIENDSHIP(USERID1, USERID2) VALUES(USERID1, USERID2); "
					+ "	INSERT INTO %s.FRIENDSHIP(USERID1, USERID2) VALUES(USERID2, USERID1); "
					+ "	UPDATE %s.USERS "
					+ " SET FRND_CNT = (SELECT FRND_CNT + 1 FROM %s.USERS WHERE UID = USERID1),	"
					+ "NO_PEND_REQ = (SELECT NO_PEND_REQ - 1 FROM %s.USERS WHERE UID = USERID1) WHERE UID = USERID1; "
					+ "	UPDATE %s.USERS "
					+ " SET FRND_CNT = (SELECT FRND_CNT + 1 FROM %s.USERS WHERE UID = USERID2), "
					+ " NO_PEND_REQ = (SELECT NO_PEND_REQ - 1 FROM %s.USERS WHERE UID = USERID2) WHERE UID = USERID2; "
					+ " END ";

			dropProcedure(stmt, db, "CreateFriendship");
			insertStoredProcedure(stmt, db, "CreateFriendhip", String.format(
					createFriendshipProcedure, db, db, db, db, db, db, db, db,
					db, db, db));

			String createPendingFriendshipProcedure = "CREATE PROCEDURE %s.CreatePendingFriendship (IN USERID1 int, IN USERID2 int) "
					+ "BEGIN "
					+ "	INSERT INTO %s.PENDING_FRIENDSHIP(USERID1, USERID2) VALUES(USERID1, USERID2); "
					+ "	INSERT INTO %s.PENDING_FRIENDSHIP(USERID1, USERID2) VALUES(USERID2, USERID1); "
					+ "	UPDATE %s.USERS SET FRND_CNT = (SELECT FRND_CNT + 1 FROM %s.USERS WHERE UID = USERID1)	WHERE UID = USERID1; "
					+ "	UPDATE %s.USERS SET FRND_CNT = (SELECT FRND_CNT + 1 FROM %s.USERS WHERE UID = USERID2)	WHERE UID = USERID2; "
					+ " END ";

			dropProcedure(stmt, db, "CreatePendingFriendship");
			insertStoredProcedure(stmt, db, "CreatePendingFriendhip",
					String.format(createPendingFriendshipProcedure, db, db, db,
							db, db, db, db));

			String resourceQuery = "CREATE PROCEDURE %s.InsertResource (IN CID int, IN WID int, TYPE VARCHAR(200), BODY VARCHAR(200), DOC VARCHAR(200)) "
					+ " BEGIN "
					+ " INSERT INTO %s.RESOURCES(CREATORID, WALLUSERID, TYPE, BODY, DOC) VALUES(CID, WID, TYPE, BODY, DOC);"
					+ " UPDATE %s.USERS SET RSRC_CNT = (SELECT RSRC_CNT + 1 FROM %s.USERS WHERE UID = CID) WHERE UID = CID;"
					+ " END";

			dropProcedure(stmt, db, "InsertResource");
			insertStoredProcedure(stmt, db, "InsertResource",
					String.format(resourceQuery, db, db, db, db));

			String pndgFrndshpUid1Idx = "CREATE INDEX %s.PNDG_FRNDSHP_UID1_IDX ON %s.PENDING_FRIENDSHIP(USERID1)";
			String pndgFrndshpUid2Idx = "CREATE INDEX %s.PNDG_FRNDSHP_UID2_IDX ON %s.PENDING_FRIENDSHIP(USERID2)";
			String frndshpUid1Idx = "CREATE INDEX %s.FRNDSHP_UID1_IDX ON %s.FRIENDSHIP(USERID1)";
			String frndshpUid2Idx = "CREATE INDEX %s.FRNDSHP_UID2_IDX ON %s.FRIENDSHIP(USERID2)";
			String rsrcRidIdx = "CREATE INDEX %s.RSRC_RID_IDX ON %s.RESOURCES(RID)";

			dropIndex(stmt, db, "PNDG_FRNDSHP_UID1_IDX");
			insertIndex(stmt, db, "PNDG_FRNDSHP_UID1_IDX",
					String.format(pndgFrndshpUid1Idx, db, db));

			dropIndex(stmt, db, "PNDG_FRNDSHP_UID2_IDX");
			insertIndex(stmt, db, "PNDG_FRNDSHP_UID2_IDX",
					String.format(pndgFrndshpUid2Idx, db, db));

			dropIndex(stmt, db, "FRNDSHP_UID2_IDX");
			insertIndex(stmt, db, "FRNDSHP_UID2_IDX",
					String.format(frndshpUid1Idx, db, db));

			dropIndex(stmt, db, "FRNDSHP_UID1_IDX");
			insertIndex(stmt, db, "FRNDSHP_UID1_IDX",
					String.format(frndshpUid2Idx, db, db));

			dropIndex(stmt, db, "RSRC_RID_IDX");
			insertIndex(stmt, db, "RSRC_RID_IDX",
					String.format(rsrcRidIdx, db, db));

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

	private void insertStoredProcedure(Statement stmt, String db,
			String procedure, String sql) {
		try {
			System.out.println(sql);
			stmt.executeUpdate(sql);
			System.out.println(String.format("%s.%s CREATED", db, procedure));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void insertIndex(Statement stmt, String db, String index, String sql) {
		try {
			System.out.println(sql);
			stmt.executeUpdate(sql);
			System.out.println(String.format("%s.%s CREATED", db, index));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void dropProcedure(Statement stmt, String db, String procedure) {
		String query = String.format("DROP PROCEDURE %s.%s", db.toUpperCase(),
				procedure);
		try {
			stmt.executeUpdate(query);
			System.out.println(String.format("DROPPED PROCEDURE %s.%s", db,
					procedure));
		} catch (SQLException e) {
			System.out.println(String.format("%s.%s PROCEDURE does not exist",
					db, procedure));
		}

	}

	private void dropIndex(Statement stmt, String db, String index) {
		String query = String.format("DROP INDEX %s.%s", db.toUpperCase(),
				index);
		try {
			stmt.executeUpdate(query);
			System.out.println(String.format("DROPPED INDEX %s.%s", db, index));
		} catch (SQLException e) {
			System.out.println(String.format("%s.%s INDEX does not exist", db,
					index));
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
		long start = System.currentTimeMillis();
		String call = String.format("CALL %s.CreateFriendship(?,?)", dbname);
		java.sql.CallableStatement proc = null;
		try {
			proc = conn.prepareCall(call);
			proc.setInt(1, friendid1);
			proc.setInt(2, friendid2);
			proc.executeUpdate();
		} catch (SQLException e2) {
			e2.printStackTrace();
		} finally {
			try {
				proc.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		long end = System.currentTimeMillis();
		long diff = end - start;
		System.out.println("CreateFriendShip " + friendid1 + " " + friendid2
				+ " " + diff);
		return 0;
	}

	public int acceptFriend(int friendid1, int friendid2) {
		return CreateFriendship(friendid1, friendid2);
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		return 0;
	}

	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage,
			int imageSize) {
		// System.out.println(entitySet);

		if (entitySet.equals("users")) {
			insertUsers(values, dbname, insertImage, imageSize);
		} else if (entitySet.equals("resources")) {
			insertResources(values, dbname);
		}

		return 0;
	}

	private void insertResources(HashMap<String, ByteIterator> values, String db) {
		long start = System.currentTimeMillis();
		String call = String
				.format("CALL %s.InsertResource(?,?,?,?,?)", dbname);
		java.sql.CallableStatement proc = null;
		int creatorid = 0;
		try {
			proc = conn.prepareCall(call);
			int cnt = 1;
			for (Entry<String, ByteIterator> entry : values.entrySet()) {
				String key = entry.getKey();
				String v = entry.getValue().toString();
				if (key.equals("creatorid") || key.equals("walluserid")) {
					if (key.equals("creatorid")) {
						creatorid = Integer.parseInt(v);
					}
					proc.setInt(cnt, Integer.parseInt(v));
				} else {
					String r = v.substring(0, Math.min(190, v.length()));
					proc.setString(cnt, r);
				}
				cnt++;
			}
			proc.executeUpdate();
		} catch (SQLException e2) {
			e2.printStackTrace();
		} finally {
			try {
				proc.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		long end = System.currentTimeMillis();
		long diff = end - start;
		System.out.println("InsertResource " + creatorid + " " + diff);
	}

	private void insertUsers(HashMap<String, ByteIterator> values, String db,
			boolean insertImage, int imageSize) {
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
		fields += ",NO_PEND_REQ,FRND_CNT,RSRC_CNT";
		if (insertImage) {
			fields += ",PIC,TPIC";
		}

		fields += ")";
		sqlValues += ",?,?,?";
		if (insertImage) {
			sqlValues += ",?,?";
		}
		sqlValues += ")";
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
			if (insertImage) {
				File image = new File(imagepath + "userpic" + imageSize
						+ ".bmp");
				try {
					FileInputStream fis = new FileInputStream(image);
					preparedStatement.setBinaryStream(cnt++, (InputStream) fis,
							(int) (image.length()));
					File thumbimage = new File(imagepath + "userpic1.bmp");
					FileInputStream fist;
					fist = new FileInputStream(thumbimage);
					preparedStatement.setBinaryStream(cnt++,
							(InputStream) fist, (int) (thumbimage.length()));
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
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

	public int inviteFriend(int friendid1, int friendid2) {
		long start = System.currentTimeMillis();
		String call = String.format("CALL %s.CreatePendingFriendship(?,?)",
				dbname);
		java.sql.CallableStatement proc = null;
		try {
			proc = conn.prepareCall(call);
			proc.setInt(1, friendid1);
			proc.setInt(2, friendid2);
			proc.executeUpdate();
		} catch (SQLException e2) {
			e2.printStackTrace();
		} finally {
			try {
				proc.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		long end = System.currentTimeMillis();
		long diff = end - start;
		System.out.println("CreatePendingFriendShip " + friendid1 + " "
				+ friendid2 + " " + diff);
		return 0;
	}

	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {

		String query = String
				.format("select u.* from %s.users u, %s.friendship f where f.userid1 = ? and f.userid2 = u.uid",
						dbname, dbname);

		PreparedStatement statement = null;
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
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		System.out.println("listing all friends");

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
					String value = null;
					if (col_name.equalsIgnoreCase("PIC")
							|| col_name.equalsIgnoreCase("TPIC")) {
						Blob aBlob = rs.getBlob(col_name);
						byte[] allBytesInBlob = aBlob.getBytes(1,
								(int) aBlob.length());
						value = allBytesInBlob.toString();
					} else {
						value = rs.getString(col_name);
					}
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

	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID, HashMap<String, ByteIterator> values) {
		System.out.println("Insert manipulation " + commentCreatorID);

		String insertQuery = String
				.format("insert into %s.MANIPULATION(CREATORID,RID,MODIFIERID,MTIME,TYPE,CONTENT) VALUES(?,?,?,CURRENT TIMESTAMP,'C',?)",
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
		System.out.println("query confirmed friendhsips");
		return 0;
	}

	@Override
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {
		System.out.println("query pending friendships");
		return 0;
	}

	public int rejectFriend(int friendid1, int friendid2) {
		System.out.println("reject pending friendship " + friendid1 + " "
				+ friendid2);

		String decreasePendingRequestsSql = String
				.format("update %s.USERS set "
						+ "NO_PEND_REQ = (select NO_PEND_REQ - 1 from %s.Users where UID = ?) "
						+ "where UID=?", dbname, dbname);

		String increaseNumberOfRejectsSql = String
				.format("update %s.USERS set "
						+ "NO_PEND_REQ = (select NO_PEN_REQ - 1 from %s.Users where UID = ?), "
						+ "NO_REQ_REJECT = (select NO_REQ_REJECT + 1 from %s.Users where UID = ?) "
						+ "where UID=?", dbname, dbname, dbname);

		String deletePendingFriendshipQuery = String
				.format("delete from %s.PENDING_FRIENDSHIP(USERID1, USERID2) where userid1=? and userid2=?",
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

	public int thawFriendship(int friendid1, int friendid2) {
		String deleteQuery = String.format(
				"delete from %s.FRIENDSHIP where userid1 = ? and userid2 = ?",
				dbname);

		String updateFriendCountSql = String
				.format("update %s.USERS "
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

		System.out.println("thaw friendship");
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

		System.out.println("view comment on resource");

		return 0;
	}

	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage,
			boolean testMode) {

		System.out.println("view friend req");

		String query = String
				.format("select u.* from %s.users u, %s.PENDING_FRIENDSHIP f where f.userid1 = ? and f.userid2 = u.uid",
						dbname, dbname);

		PreparedStatement statement = null;
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
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		System.out.println("view friend req end");

		if (error)
			return 1;
		return 0;
	}

	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		// "friendcount", "resourcecount" and "pendingcount"
		String query = String
				.format("select FRND_CNT, RSRC_CNT, NO_PEND_REQ from %s.users where uid = ?",
						dbname);
		PreparedStatement statement = null;

		try {
			statement = conn.prepareStatement(query);
			statement.setInt(1, profileOwnerID);
			ResultSet rs = statement.executeQuery();
			int v = populateUserRecord(result, rs);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (requesterID != profileOwnerID) {
			if (result.containsKey("pendingcount"))
				result.remove("pendingcount");
		}

		System.out.println("view profile");

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
						result.put("friendcount", new StringByteIterator(value));
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

	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		String query = String
				.format("select * from %s.resources where ( creatorid = %s or walluserid = %s ) order by RID DESC FETCH FIRST %s ROWS ONLY",
						dbname, profileOwnerID, profileOwnerID, k);

		Statement statement = null;
		try {
			statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			int v = populateTopKUserRecord(result, rs);
		} catch (SQLException e) {
			e.printStackTrace();
			return 1;
		} finally {
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		System.out.println("view top k resources");
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

	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values, boolean insertImage,
			int imageSize) {
		return insertEntity(table, key, values, insertImage, imageSize);
	}

	@Override
	public int getUserProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		return viewProfile(requesterID, profileOwnerID, result, insertImage,
				testMode);
	}

	@Override
	public int getListOfFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		return listFriends(requesterID, profileOwnerID, fields, result,
				insertImage, testMode);
	}

	@Override
	public int viewPendingRequests(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> values, boolean insertImage,
			boolean testMode) {
		return viewPendingRequests(profileOwnerID, values, insertImage,
				testMode);
	}

	@Override
	public int acceptFriendRequest(int invitorID, int inviteeID) {
		return acceptFriendRequest(invitorID, inviteeID);
	}

	@Override
	public int rejectFriendRequest(int invitorID, int inviteeID) {
		return rejectFriendRequest(invitorID, inviteeID);
	}

	@Override
	public int inviteFriends(int invitorID, int inviteeID) {
		return inviteFriend(invitorID, inviteeID);
	}

	@Override
	public int getTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		return viewTopKResources(requesterID, profileOwnerID, k, result);
	}

	@Override
	public int getResourceComments(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		return viewCommentOnResource(requesterID, profileOwnerID, resourceID,
				result);
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID) {
		return postCommentOnResource(commentCreatorID, profileOwnerID,
				resourceID);
	}

	@Override
	public int unFriendFriend(int friendid1, int friendid2) {
		return thawFriendship(friendid1, friendid2);
	}
}