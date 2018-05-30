package kx.examples;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import kx.c;
import kx.c.KException;

/**
 * Example logic for demonstrating kdb+ connecting to Java acting as a server. This will handle a single incoming kdb+ connection.
 * 
 * @author plyness
 *
 */
public class IncomingConnectionExample {

	public static void main(String[] args) {
		try {
			new IncomingConnectionExample().start();
		} catch (KException | IOException e) {
			System.err.println("Error occured in execution - Details:");
			e.printStackTrace();
		}
	}

	private void start() throws KException, IOException {
		// Wait for incoming connection
		System.out.println("Waiting for incoming connection on port 5001..");
		c incomingConnection = new c(new ServerSocket(5001));
		// once established, start listening loop
		System.out.println("Connection established - waiting for ping.");
		while (true) {
			Object incoming = incomingConnection.k();
			try {
				// check the incoming object and return something based on what it is
				if (incoming instanceof String && ((String) incoming).equals("returnError")) {
					incomingConnection
							.ke("Incoming connection: example of returning an error signal!");

				} else if(incoming.getClass().isArray()) {
					//If array, use Arrays.toString
					incomingConnection.kr("The incoming list values were: "+Arrays.toString((Object[])incoming));
					
				} else {
					incomingConnection.kr(("The incoming message was: " + incoming.toString()).toCharArray());
				}
			} catch (IOException e) {
				// return error responses too
				incomingConnection.ke(e.getMessage());
			}
		}

	}
	
}
