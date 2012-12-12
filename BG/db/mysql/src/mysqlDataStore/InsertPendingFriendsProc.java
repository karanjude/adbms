package mysqlDataStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertPendingFriendsProc {

	public final String sql = new String(
			"INSERT INTO Friendship VALUES (?, ?, ?);");

	public final String sql2 = new String(
			"UPDATE Users SET pendingFriends = pendingFriends +1 WHERE userid= ?;");

	private final Connection conn;

	public InsertPendingFriendsProc(Connection conn) {
		this.conn = conn;
	}

	public void execute(int invitorID, int inviteeID, int status) {
		PreparedStatement preparedStatement1 = null;
		PreparedStatement preparedStatement2 = null;
		try {
			conn.setAutoCommit(false);
			preparedStatement1 = conn.prepareStatement(sql);
			preparedStatement1.setInt(1, invitorID);
			preparedStatement1.setInt(2, inviteeID);
			preparedStatement1.setInt(3, status);

			preparedStatement2 = conn.prepareStatement(sql2);
			preparedStatement2.setInt(1, invitorID);

			preparedStatement1.executeUpdate();
			preparedStatement2.executeUpdate();
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
				if (null != preparedStatement1)
					preparedStatement1.close();
				if (null != preparedStatement2)
					preparedStatement2.close();
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
