package mysqlDataStore;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PendingFriendsPerUserProc {

	private final Connection conn;

	public final String sql = new String(
			"SELECT pendingFriends FROM Users where userid = %s;");

	public PendingFriendsPerUserProc(Connection conn) {
		this.conn = conn;
	}

	public Integer execute(Integer userid) {
		java.sql.Statement st = null;
		ResultSet rs = null;
		String query = String.format(sql, userid);
		try {
			st = conn.createStatement();
			rs = st.executeQuery(query);
			if (null != rs && rs.next()) {
				return Integer.parseInt(rs.getString(1));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (null != rs) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != st) {
				try {
					st.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return 0;
	}

}
