package mysqlDataStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Vector;

import org.voltdb.SQLStmt;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

public class GetCreatedResourcesProc {

	public final String sql = new String(
			"SELECT * FROM Resource WHERE creatorid = ?;");
	private final Connection conn;

	public GetCreatedResourcesProc(Connection conn) {
		this.conn = conn;
	}

	public void execute(int resourceCreatorID,
			Vector<HashMap<String, ByteIterator>> result) {

		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		try {
			preparedStatement = conn.prepareStatement(sql);
			preparedStatement.setInt(1, resourceCreatorID);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				// get the number of columns and their names
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					values.put(col_name, new StringByteIterator(value));
				}
				result.add(values);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != rs)
					rs.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			try {
				if (null != preparedStatement)
					preparedStatement.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
