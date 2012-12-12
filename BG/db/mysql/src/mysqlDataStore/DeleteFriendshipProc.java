package mysqlDataStore;

import java.sql.Connection;
import java.sql.SQLException;

public class DeleteFriendshipProc {

	private final Connection conn;

	public final String sql = new String(
			"DELETE FROM Friendship WHERE userid1=? and userid2= ? and status=1;");

	public DeleteFriendshipProc(Connection conn) {
		this.conn = conn;
	}

	public void execute(int invitorID, int inviteeID) {
		System.out.println("Delete Friendship");
		java.sql.PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(sql);
			statement.setInt(1, invitorID);
			statement.setInt(2, inviteeID);
			statement.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != statement)
					statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
