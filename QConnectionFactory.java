package kx.examples;

import java.io.IOException;

import kx.c;
import kx.c.KException;

/**
 * Factory class for production of c objects with the specified parameters. 
 * @author plyness
 *
 */
public class QConnectionFactory {

	private String host;
	private int port;
	private String username;
	private String password;
	private boolean useTLS;
	
	public QConnectionFactory(String host, int port, String username, String password, boolean useTLS) {
		this.host=host;
		this.port=port;
		this.username=username;
		this.password=password;
		this.useTLS=useTLS;
	}
	
	public QConnectionFactory(String host, int port, String username, String password) {
		this(host,port,username,password,false);
	}
	
	public QConnectionFactory(String host, int port) {
		this(host,port,"","");
	}
	
	public c getQConnection() throws KException, IOException {
		return new c(host,port,username+":"+password,useTLS);
	}
	
	public static QConnectionFactory getDefault() {
		return new QConnectionFactory("localhost", 10000);
	}
	

}
