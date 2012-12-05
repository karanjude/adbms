package edu.bg.verticalscaling;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

public class BGVerticalDataStortController {

	private final BGVerticalScaleOptions options;

	public BGVerticalDataStortController(BGVerticalScaleOptions options) {
		this.options = options;
	}

	public void start() {
		try {
			TServerSocket serverTransport = new TServerSocket(7911);
			BGService.Processor<BGService.Iface> processor = new BGService.Processor<BGService.Iface>(
					new BGVerticalServiceImpl(options));
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(
					serverTransport).processor(processor));
			System.out.println("Starting Server on port 7911..");
			server.serve();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
	}
}
