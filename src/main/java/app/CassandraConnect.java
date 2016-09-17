package app;

import com.datastax.driver.core.*;

public class CassandraConnect {

	private Cluster cluster;
	private Session session;
	
	public CassandraConnect(String node, int port, String keyspace) {
		this.cluster = Cluster.builder()
				.addContactPoint(node)
				.withPort(port)
				.build();
	    this.session = cluster.connect(keyspace);
	}
	
	public Session getSession() {
		return this.session;
	}
	
	public void close() {
		this.cluster.close();
		this.session.close();
	}
}
