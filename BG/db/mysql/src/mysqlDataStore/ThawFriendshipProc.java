package mysqlDataStore;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class ThawFriendshipProc {

	private final Connection conn;

	public final String sql1 = new String(
			"DELETE FROM Friendship WHERE (userid1=? and userid2= ?) OR (userid1=? and userid2= ?) and status=2;");
	public final String sql2 = new String(
			"UPDATE Users SET confirmedFriends = confirmedFriends - 1 WHERE userid= ? OR userid=?;");

	public ThawFriendshipProc(Connection conn) {
		this.conn = conn;
	}

	public void execute(int friendid1, int friendid2, int friendid22,
			int friendid21) {
		String query1 = String.format(sql1, friendid1, friendid2, friendid22,
				friendid21);
		String query2 = String.format(sql2, friendid1, friendid22);
		java.sql.Statement statement = null;
		Statement statement1 = null;
		try {
			conn.setAutoCommit(false);
			statement = conn.createStatement();
			statement.executeUpdate(query1);
			statement1 = conn.createStatement();
			statement1.executeUpdate(query2);
			conn.commit();
		} catch (SQLException e) {
			if (null != conn)
				try {
					conn.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			e.printStackTrace();
		} finally {
			try {
				if (null != statement)
					statement.close();
				if (null != statement1)
					statement1.close();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
