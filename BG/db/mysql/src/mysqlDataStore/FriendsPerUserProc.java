package mysqlDataStore;

import java.sql.Connection;

public class FriendsPerUserProc {

	private final Connection conn;

	public FriendsPerUserProc(Connection conn) {
		this.conn = conn;
	}

	public Integer execute(Integer offset) {
		
		return 0;
	}

}
