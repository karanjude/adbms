package mysqlDataStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertFriendsProc {

	private final Connection conn;

	public final String sql1 = new String(
			"INSERT INTO Friendship VALUES (?, ?, ?);");

	public final String sql2 = new String(
			"UPDATE Users SET confirmedFriends = confirmedFriends +1 WHERE userid= ?;");

	public InsertFriendsProc(Connection conn) {
		this.conn = conn;
	}

	public void execute(int friendid1, int friendid2, int status) {
		String query1 = String.format(sql1, friendid1, friendid2, status);
		String query2 = String.format(sql2, friendid1);
		String query3 = String.format(sql1, friendid2, friendid1, status);
		String query4 = String.format(sql2, friendid2);

		PreparedStatement statement1 = null;
		PreparedStatement statement2 = null;
		PreparedStatement statement3 = null;
		PreparedStatement statement4 = null;

		try {
			conn.setAutoCommit(false);
			statement1 = conn.prepareStatement(query1);
			statement1.setInt(1, friendid1);
			statement1.setInt(2, friendid2);
			statement1.setInt(3, status);
			statement1.executeUpdate();

			statement2 = conn.prepareStatement(query2);
			statement2.setInt(1, friendid1);
			statement2.executeUpdate();

			statement3 = conn.prepareStatement(query3);
			statement3.setInt(1, friendid2);
			statement3.setInt(2, friendid1);
			statement3.setInt(3, status);
			statement3.executeUpdate();

			statement4 = conn.prepareStatement(query4);
			statement4.setInt(1, friendid2);
			statement4.executeUpdate();
			conn.commit();

		} catch (SQLException e) {
			e.printStackTrace();
			if (null != conn)
				try {
					conn.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		} finally {
			try {
				if (null != statement1)
					statement1.close();
				if (null != statement2)
					statement2.close();
				if (null != statement3)
					statement3.close();
				if (null != statement4)
					statement4.close();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
