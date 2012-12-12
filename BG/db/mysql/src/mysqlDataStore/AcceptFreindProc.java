package mysqlDataStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AcceptFreindProc {

	private final Connection conn;

	public final String sql1 = new String(
			"UPDATE Friendship SET status = 2 WHERE userid1=? and userid2= ?;");
	public final String sql2 = new String(
			"UPDATE Users SET pendingFriends = pendingFriends - 1 WHERE userid= ?;");
	public final String sql3 = new String(
			"UPDATE Users SET confirmedFriends = confirmedFriends +1 WHERE userid= ?;");

	public final String sql4 = new String(
			"UPDATE Users SET confirmedFriends = confirmedFriends +1 WHERE userid= ?;");

	public AcceptFreindProc(Connection conn) {
		this.conn = conn;
	}

	public void execute(int invitorID, int inviteeID) {
		System.out.println("Accept Friend");
		PreparedStatement statement1 = null;
		PreparedStatement statement2 = null;
		PreparedStatement statement3 = null;
		PreparedStatement statement4 = null;

		try {
			conn.setAutoCommit(false);
			statement1 = conn.prepareStatement(sql1);
			statement1.setInt(1, invitorID);
			statement1.setInt(2, inviteeID);
			statement1.executeUpdate();

			statement2 = conn.prepareStatement(sql2);
			statement2.setInt(1, invitorID);
			statement2.executeUpdate();

			statement3 = conn.prepareStatement(sql3);
			statement3.setInt(1, invitorID);
			statement3.executeUpdate();

			statement4 = conn.prepareStatement(sql4);
			statement4.setInt(1, inviteeID);
			statement4.executeUpdate();
			conn.commit();
			conn.setAutoCommit(true);

		} catch (SQLException e) {
			e.printStackTrace();
			if (null != conn)
				try {
					conn.rollback();
				} catch (SQLException e1) {
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
