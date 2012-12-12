package mysqlDataStore;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class UserOffsetProc {

	private final Connection conn;

	public final String sql = new String("SELECT min(userid) FROM Users;");

	public UserOffsetProc(Connection conn) {
		this.conn = conn;
	}

	public Integer execute() {
		Statement statement = null;
		ResultSet rs = null;
		Integer result = 0;
		try {
			statement = conn.createStatement();
			rs = statement.executeQuery(sql);
			if (rs.next()) {
				String r = rs.getString(1);
				if (null == r || r.equalsIgnoreCase("null")) {
					result = 0;
				} else {
					result = Integer.parseInt(r);
				}
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
