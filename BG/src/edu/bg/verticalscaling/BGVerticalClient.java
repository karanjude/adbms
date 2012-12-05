package edu.bg.verticalscaling;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class BGVerticalClient {
	private TTransport transport;
	private TProtocol protocol;
	private BGService.Client client;

	public BGVerticalClient() {
		transport = new TSocket("0.0.0.0", 7911);
		protocol = new TBinaryProtocol(transport);
		client = new BGService.Client(protocol);
	}

	public void startDataStore(String dataStore, Integer ram, Integer cores,
			Integer threads) {
		try {
			transport.open();
			client.start(dataStore, ram, cores, threads);
			transport.close();
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}

	}

	public void stopDataStore(String dataStore) {
		try {
			transport.open();
			client.stop(dataStore);
			transport.close();
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}

	}

	public void exit() {
		try {
			transport.open();
			client.exit();
		} catch (TException e) {
			System.out.println("Server ShutDown");
		} finally {
			transport.close();
		}

	}
}
