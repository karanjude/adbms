package mysqlDataStore;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ResourceCountProc {

	private final Connection conn;

	public final String sql = new String(
			"SELECT resourceCount FROM Users  where userid = %s;");

	public ResourceCountProc(Connection conn) {
		this.conn = conn;
	}

	public Integer execute(Integer userid) {
		Statement statement = null;
		ResultSet rs = null;
		Integer result = 0;
		String query = String.format(sql, userid);
		try {
			statement = conn.createStatement();
			rs = statement.executeQuery(query);
			if (null != rs && rs.next()) {
				result = Integer.parseInt(rs.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (null != rs)
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if (null != statement)
				try {
					statement.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return result;
	}

}
