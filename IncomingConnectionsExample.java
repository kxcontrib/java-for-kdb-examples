package kx.examples;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;

import kx.c;
import kx.c.KException;

/**
 * Example logic for demonstrating kdb+ connecting to Java acting as a server.
 * This will handle multiple incoming kdb+ connections.
 * 
 * 
 * @author plyness
 *
 */
public class IncomingConnectionsExample {

	public IncomingConnectionsExample() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		try {
			new IncomingConnectionsExample().start();
		} catch (KException | IOException e) {
			System.err.println("Error occured in execution - Details:");
			e.printStackTrace();
		}

	}

	/**
	 * This is designed to be presented concisely as one method, and there are a
	 * number of ways in which it could be improved. Thread management could be
	 * introduced by means of thread pooling, and in a production-ready
	 * application a safe shutdown hook or other such mechanism would be
	 * expected as well. For the sake of presenting the core logic as clearly as
	 * possible, such elements are ommitted from this example.
	 * 
	 * @throws KException
	 * @throws IOException
	 */
	private void start() throws KException, IOException {
		// Create server socket reference beforehand..
		ServerSocket serverSocket = new ServerSocket(5001);
		// Establish outgoing connection
		System.out.println("Accepting incoming connections..");
		// set up connection loop
		while (true) {
			// Create c object with reference to server socket.
			c incomingConnection = new c(serverSocket);
			// Create thread
			new Thread(new Runnable() {
				@Override
				public void run() {
					while (true) {
						//Logic in this loop is similar to the single connection
						try {
							Object incoming = incomingConnection.k();

							if (incoming instanceof String
									&& ((String) incoming)
											.equals("returnError")) {
								incomingConnection
										.ke("Incoming connection: example of returning an error signal!");
							} else if (incoming.getClass().isArray()) {
								incomingConnection.kr("The incoming list values were: "
										+ Arrays.toString((Object[]) incoming));

							} else {
								incomingConnection
										.kr(("The incoming message was: " + incoming
												.toString()).toCharArray());
							}
						} catch (IOException | KException e) {
							// return error responses too
							try {
								incomingConnection.ke(e.getMessage());
							} catch (IOException e1) {
								e1.printStackTrace();
							}
						}
					}

				}
			}).start();

		}

	}

}
