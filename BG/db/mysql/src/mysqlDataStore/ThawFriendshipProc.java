package mysqlDataStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ThawFriendshipProc {

	private final Connection conn;

	public final String sql1 = new String(
			"DELETE FROM friendship WHERE (userid1=? and userid2= ?) OR (userid1=? and userid2= ?) and status=2;");
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
		PreparedStatement statement = null;
		PreparedStatement statement1 = null;
		try {
			statement = conn.prepareStatement(query1);
			statement.executeQuery();
			statement1 = conn.prepareStatement(query2);
			statement1.executeQuery();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				statement.close();
				statement1.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
