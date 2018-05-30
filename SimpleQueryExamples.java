package kx.examples;

import java.io.IOException;

import kx.c;
import kx.c.KException;

/**
 * Example method demonstrating basic query execution against a remote kdb+ instance
 * @author plyness
 *
 */
public class SimpleQueryExamples {

	private c qConnection;
	
	public SimpleQueryExamples() throws KException, IOException {
		//Localhost, port 10000
		qConnection = QConnectionFactory.getDefault().getQConnection();
	}
	
	
	public static void main(String[] args) {
		
		
		try {
			new SimpleQueryExamples().start();
		} catch (KException | IOException e) {
			System.err.println("Error occured in execution - Details:");
			e.printStackTrace();
		}
		
		
	}
	
	
	public void start() throws KException, IOException {
		//Object for storing the results of these queries
		Object result = null;
		
		//Basic synchronous q expression
		result = qConnection.k("{x+y}[4;3]");
		System.out.println(result.toString());
		
		//parameterised synchronous query
		result = qConnection.k("{x+y}",4,3); //Note autoboxing!
		System.out.println(result.toString());
		
		//asynchronous assignment of function
		qConnection.ks("jFunc:{x-y+z}");
		
		//synchronous calling of that function
		result = qConnection.k("jFunc",10,4,3);
		System.out.println(result);
		//asynchronous error - note no exception can be returned, so be careful!
		qConnection.ks("{x+y}[4;3;2]");
		
		//Always close resources!
		qConnection.close();
	}

}
