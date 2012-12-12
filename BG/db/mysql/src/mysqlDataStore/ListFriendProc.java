package mysqlDataStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

public class ListFriendProc {

	public final String sql = new String(
			"SELECT userid, inviterid, inviteeid, username, fname, "
					+ "lname, gender, dob, jdate, ldate, address,email,tel"
					+ " FROM Users, Friendship WHERE (inviterid=? and userid=inviteeid) and status = 2;");
	private final Connection conn;

	public ListFriendProc(Connection conn) {
		this.conn = conn;
	}

	public int execute(int requesterID,
			Vector<HashMap<String, ByteIterator>> result, Set<String> fields) {

		int retVal = 0;

		ResultSet rs = null;
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setInt(1, requesterID);
			rs = preparedStatement.executeQuery();
			int cnt = 0;
			while (rs.next()) {
				cnt++;
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				if (fields != null) {
					for (String field : fields) {
						String value = rs.getString(field);
						values.put(field, new StringByteIterator(value));
					}
					result.add(values);
				} else {
					// get the number of columns and their names
					// Statement st = conn.createStatement();
					// ResultSet rst = st.executeQuery("SELECT * FROM users");
					ResultSetMetaData md = rs.getMetaData();
					int col = md.getColumnCount();
					for (int i = 1; i <= col; i++) {
						String col_name = md.getColumnName(i);
						String value = rs.getString(col_name);
						values.put(col_name, new StringByteIterator(value));
					}
					result.add(values);
				}
			}
		} catch (SQLException e) {
			retVal = -2;
			e.printStackTrace();
		} finally {
			try {
				if (null != rs)
					rs.close();
				if (null != preparedStatement)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace();
			}
		}

		return retVal;
	}
}
