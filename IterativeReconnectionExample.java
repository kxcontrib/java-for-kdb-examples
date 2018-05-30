package kx.examples;

import java.io.IOException;

import kx.c;
import kx.c.KException;

/**
 * Example of the use of infinite loops and error handling to implement reconnection logic for a c object.
 * @author plyness
 *
 */
public class IterativeReconnectionExample {

	private QConnectionFactory qConnFactory;
	private c qConnection;

	public IterativeReconnectionExample() throws InterruptedException {
		qConnFactory = QConnectionFactory.getDefault();
			while(true) {
				try {
					qConnection = qConnFactory.getQConnection();
					System.out.println("Connection established!");
					break;
				} catch (IOException e1) {
					System.err.println("Connection failed - retrying..");
					Thread.sleep(5000);
					continue;
				} catch (KException e2) {
					System.err.println("Connection failed - retrying..");
					Thread.sleep(5000);
					continue;
				}

			}
		
	}

	public static void main(String[] args) {

		try {
			new IterativeReconnectionExample().start();
		} catch (InterruptedException e) {

		}

	}

	private void start() throws InterruptedException {
		//Assume connection already established.
		while (true) {
			try {
				//Simple ping loop
				System.out.println(qConnection.k("`ping"));
				Thread.sleep(1000);

			} catch (IOException | KException e) {
				//initiate reconnect loop.
				while (true) {
					try {
						System.err.println("Connection failed - retrying..");
						//Wait a bit before trying to reconnect
						Thread.sleep(5000);
						qConnection = qConnFactory.getQConnection();
						System.out.println("Connection re-established! Resuming..");
						break;
					} catch (IOException | KException e1) {
						//loop if it fails
						continue;
					} 
				}
			}
		}

	}





}
