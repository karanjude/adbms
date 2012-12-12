package mysqlDataStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SelectUserCountProc {

	private final Connection conn;

	public SelectUserCountProc(Connection conn) {
		this.conn = conn;
	}

	public Integer execute() {
		Statement st = null;
		ResultSet rs = null;
		try {
			st = conn.createStatement();
			String query = "SELECT count(*) from Users";
			rs = st.executeQuery(query);
			if (rs.next()) {
				return Integer.parseInt(rs.getString(1));
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (null != rs) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (null != st) {
				try {
					st.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return 0;
	}

}
