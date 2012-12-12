package mysqlDataStore;

import java.io.FileOutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;

import org.voltdb.SQLStmt;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

public class ProfileDetailsProc {

	private final Connection conn;

	public final String sql = new String(
			"SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel, confirmedFriends,pendingFriends,resourceCount FROM  Users WHERE userid = ?;");

	public ProfileDetailsProc(Connection conn) {
		this.conn = conn;
	}

	public void execute(int profileOwnerID, HashMap<String, ByteIterator> result) {
		ResultSet rs = null;
		int retVal = 0;

		String query = "";
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setInt(1, profileOwnerID);
			rs = preparedStatement.executeQuery();
			ResultSetMetaData md = rs.getMetaData();
			int col = md.getColumnCount();
			if (rs.next()) {
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					if (col_name.equalsIgnoreCase("confirmedFriends")) {
						result.put("FriendCount", new StringByteIterator(value));
					} else if (col_name.equalsIgnoreCase("pendingFriends")) {
						result.put("PendingCount",
								new StringByteIterator(value));
					} else if (col_name.equalsIgnoreCase("resourceCount")) {
						result.put("PendingCount",
								new StringByteIterator(value));
					} else {
						result.put("ResourceCount", new StringByteIterator(
								value));
					}
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != rs)
					rs.close();
				if (null != preparedStatement)
					preparedStatement.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}
