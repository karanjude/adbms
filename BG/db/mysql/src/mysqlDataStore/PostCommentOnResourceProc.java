package mysqlDataStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostCommentOnResourceProc {

	public final String sql = new String(
			"INSERT INTO Modify VALUES (?, ?, ?, 'datehihi', 'post', '1234');");
	private final Connection conn;

	public PostCommentOnResourceProc(Connection conn) {
		this.conn = conn;
	}

	public void execute(int profileOwnerID, int commentCreatorID,
			int resourceID, String string, String string2, String string3) {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setInt(1, profileOwnerID);
			preparedStatement.setInt(2, resourceID);
			preparedStatement.setInt(3, commentCreatorID);
			preparedStatement.executeUpdate();
		} catch (SQLException sx) {
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
	}

}
