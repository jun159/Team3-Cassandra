package database;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class test {
	private Cluster cluster;
	private static Session session;
	
	

	
	
	public static void main(String[] args) {

		//BinaryDriverTest bdt = new BinaryDriverTest("127.0.0.1", 9042, "Tutorial");
		

        Cluster cluster = Cluster.builder()
                          .addContactPoints("127.0.0.1")
                          .build();
        Session session = cluster.connect();

//		Session session = bdt.session;
        
		String cqlStatement = "CREATE KEYSPACE myfirstcassandradb WITH " + 
                              "replication = {'class':'SimpleStrategy','replication_factor':1}";        
        session.execute(cqlStatement);

        String cqlStatement2 = "CREATE TABLE myfirstcassandradb.users (" + 
                               " user_name varchar PRIMARY KEY," + 
                               " password varchar " + 
                               ");";
        session.execute(cqlStatement2);

        System.out.println("Done");
        System.exit(0);
		    
	
		
		
		
		
		
//		test client = new test();
//		client.connect("zhiliang@xcnd6.comp.nus.edu.sg");
////		client.createSchema();
//
//	}
//	
//	// function to connect to the cluster
//	public void connect(String node) {
//		cluster = Cluster.builder().addContactPoint(node).build();
//		Metadata metadata = cluster.getMetadata();
//		System.out.printf("Connected to cluster: %s\n",
//				metadata.getClusterName());
//		for (Host host : metadata.getAllHosts()) {
//			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
//					host.getDatacenter(), host.getAddress(), host.getRack());
//		}
//		// Get a session from your cluster and store the reference to it.
//		session = cluster.connect();
	}
	
	

}
