package soc.database;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class BinaryDriverTest {
	public Cluster cluster;
	public Session session;
	public BinaryDriverTest(String cassandraHost, int cassandraPort, String keyspaceName) {
		String m_cassandraHost = cassandraHost;
		int  m_cassandraPort = cassandraPort;
		String m_keyspaceName = keyspaceName;

	   // LOG.info("Connecting to {}:{}...", cassandraHost, cassandraPort);
	    cluster = Cluster.builder().withPort(m_cassandraPort).addContactPoint(cassandraHost).build();
	    session = cluster.connect(m_keyspaceName);
	   // LOG.info("Connected.");
	}
}
