package mysqlDataStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertResourcesProc {

	private final Connection conn;

	public final String sql = new String(
			"INSERT INTO Resource(creatorid,wallUserId,type,body,doc) VALUES (?, ?, ?, ?, ?);");
	public final String sql2 = new String(
			"UPDATE Users SET resourceCount = resourceCount+1 WHERE userid= ?;");

	public InsertResourcesProc(Connection conn) {
		this.conn = conn;
	}

	public void execute(String creatorid, String wallUsrId, String type,
			String body, String doc) {
		String query1 = String.format(sql, creatorid, wallUsrId, type, body,
				doc);
		String query2 = String.format(sql2, creatorid);

		PreparedStatement statement = null;
		PreparedStatement statement1 = null;
		try {
			statement = conn.prepareStatement(query1);
			statement.setString(1, creatorid);
			statement.setString(2, wallUsrId);
			statement.setString(3, type);
			statement.setString(4, body);
			statement.setString(5, doc);
			statement.executeUpdate();

			statement1 = conn.prepareStatement(query2);
			statement1.setString(1, creatorid);
			statement1.executeUpdate();
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
