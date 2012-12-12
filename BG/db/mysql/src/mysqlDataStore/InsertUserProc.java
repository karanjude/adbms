package mysqlDataStore;

import java.sql.Connection;
import java.sql.SQLException;

public class InsertUserProc {

	private final Connection conn;

	public InsertUserProc(Connection conn) {
		this.conn = conn;
	}

	public final String sql = new String(
			"INSERT INTO Users(username,pw,fname,lname,gender,dob,jdate,ldate,address,email,tel,confirmedFriends,pendingFriends,resourceCount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,? ,?, ?, ?);");

	public void execute(String uName, String pwd, String fname, String lname,
			String gender, String dob, String jdate, String ldate,
			String address, String email, String tel, int confirmedFriends,
			int pendingFrineds, int resourceCount) {

		String query = String.format(sql, uName, pwd, fname, lname, gender,
				dob, jdate, ldate, address, email, tel, confirmedFriends,
				pendingFrineds, resourceCount);

		java.sql.PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(query);
			statement.setString(1, uName);
			statement.setString(2, pwd);
			statement.setString(3, fname);
			statement.setString(4, lname);
			statement.setString(5, gender);
			statement.setString(6, dob);
			statement.setString(7, jdate);
			statement.setString(8, ldate);
			statement.setString(9, address);
			statement.setString(10, email);
			statement.setString(11, tel);
			statement.setInt(12, confirmedFriends);
			statement.setInt(13, pendingFrineds);
			statement.setInt(14, resourceCount);

			statement.executeUpdate();
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
