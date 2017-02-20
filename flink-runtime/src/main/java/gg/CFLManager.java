package gg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class CFLManager {

	protected static final Logger LOG = LoggerFactory.getLogger(CFLManager.class);

	static CFLManager sing = null;
	public static CFLManager getSing() {return sing;}

	static final int port = 4444;

	public static byte tmId = -1;
	public static int numAllSlots = -1;
	public static int numTaskSlotsPerTm = -1;

	public static void create() {
		sing = new CFLManager();
	}

	public static void create(String[] hosts) {
		sing = new CFLManager(hosts);
	}


	public CFLManager() {
		// local execution
		this(new String[]{});
	}

	public CFLManager(String[] hosts) {
		this.hosts = hosts;
		connReaders = new ConnReader[hosts.length];
		recvRemoteAddresses = new SocketAddress[hosts.length];

		connAccepter = new ConnAccepter(); //thread

		senderSockets = new Socket[hosts.length];
		senderStreams = new OutputStream[hosts.length];

		curCFL.add(0);

		createSenderConnections();
	}

	private String[] hosts;
	private ConnAccepter connAccepter;
	private ConnReader[] connReaders;
	private SocketAddress[] recvRemoteAddresses;

	private Socket[] senderSockets;
	private OutputStream[] senderStreams;

	private List<Integer> curCFL = new ArrayList<>();
	private List<Integer> tentativeCFL = new ArrayList<>();

	private List<CFLCallback> callbacks = new ArrayList<>();

	private void createSenderConnections() {
		final int timeout = 500;
		int i = 0;
		for (String host : hosts) {
			try {
				Socket socket;
				while(true) {
					try {
						socket = new Socket();
						socket.setPerformancePreferences(0,1,0);
						LOG.info("GGG Connecting sender connection to " + host + ".");
						socket.connect(new InetSocketAddress(host, port), timeout);
						LOG.info("GGG Sender connection connected to  " + host + ".");
						break;
					} catch (SocketTimeoutException exTimeout) {
						LOG.info("GGG Sender connection to            " + host + " timed out, retrying...");
					} catch (ConnectException ex) {
						LOG.info("GGG Sender connection to            " + host + " was refused, retrying...");
						try {
							Thread.sleep(500);
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}
					} catch (IOException e) {
						LOG.info("GGG Sender connection to            " + host + " caused an IOException, retrying... " + e);
						try {
							Thread.sleep(500);
						} catch (InterruptedException e2) {
							throw new RuntimeException(e2);
						}
					}
				}
				senderSockets[i] = socket; //new Socket(host, port);
				senderStreams[i] = socket.getOutputStream();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			i++;
		}
		LOG.info("GGG All sender connections are up.");
	}

	private void sendElement(CFLElement e) {
		final int bufLen = 8;
		byte[] buf = new byte[bufLen];
		buf[0] = (byte)(e.seqNum % 256);
		buf[1] = (byte)((e.seqNum / 256) % 256);
		buf[2] = (byte)((e.seqNum / 256 / 256) % 256);
		buf[3] = (byte)((e.seqNum / 256 / 256 / 256) % 256);
		buf[4] = (byte)(e.bbId % 256);
		buf[5] = (byte)((e.bbId / 256) % 256);
		buf[6] = (byte)((e.bbId / 256 / 256) % 256);
		buf[7] = (byte)((e.bbId / 256 / 256 / 256) % 256);
		for (int i = 0; i<hosts.length; i++) {
			try {
				senderStreams[i].write(buf);
				senderStreams[i].flush();
			} catch (IOException e1) {
				throw new RuntimeException(e1);
			}
		}
	}

	private class ConnAccepter implements Runnable {

		Thread thread;

		public ConnAccepter() {
			thread = new Thread(this, "ConnAccepter");
			thread.start();
		}

		@Override
		public void run() {
			ServerSocket serverSocket;
			try {
				serverSocket = new ServerSocket(port);
				int i = 0;
				while(i < hosts.length) {
					LOG.info("GGG Listening for incoming connections " + i);
					Socket socket = serverSocket.accept();
					SocketAddress remoteAddr = socket.getRemoteSocketAddress();
					LOG.info("GGG Got incoming connection " + i + " from " + remoteAddr);
					recvRemoteAddresses[i] = remoteAddr;
					connReaders[i] = new ConnReader(socket, i);
					i++;
				}
				LOG.info("GGG All incoming connections connected");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private class ConnReader implements Runnable {

		Thread thread;

		Socket socket;

		public ConnReader(Socket socket, int i) {
			this.socket = socket;
			thread = new Thread(this, "ConnReader_" + i);
			thread.start();
		}

		@Override
		public void run() {
			try {
				InputStream ins = socket.getInputStream();
				//InputStreamReader insr = new InputStreamReader(ins);
				//BufferedReader inbr = new BufferedReader(insr);
				while(true){
					final int bufLen = 8;
					byte[] buf = new byte[bufLen];
					int i;
					for(i=0; i<bufLen;){
						int numRead = ins.read(buf,i,bufLen-i);
						if(numRead == -1) {
							// connection was closed
							LOG.info("GGG Connection to " + socket.getRemoteSocketAddress() + " was closed remotely (asszem).");
							return;
						}
						i += numRead;
					}
					assert i == bufLen;

					assert ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
					// little endian: a szam vege van a kisebb cimeken
					int seqNum = buf[0] + 256 * buf[1] + 256 * 256 * buf[2] + 256 * 256 * 256 * buf[3];
					int bbId = buf[4] + 256 * buf[5] + 256 * 256 * buf[6] + 256 * 256 * 256 * buf[7];
					CFLElement e = new CFLElement(seqNum, bbId);
					LOG.info("GGG Got " + e);
					addTentative(seqNum, bbId); // will do the callbacks
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private synchronized void addTentative(int seqNum, int bbId) {
		while (seqNum >= tentativeCFL.size()) {
			tentativeCFL.add(null);
		}
		tentativeCFL.set(seqNum, bbId);

		for (int i = curCFL.size(); i < tentativeCFL.size(); i++) {
			Integer t = tentativeCFL.get(i);
			if (t == null)
				break;
			curCFL.add(t);
			LOG.info("GGG Adding BBID " + t + " to CFL");
			notifyCallbacks();
			// szoval minden elemnel kuldunk kulon, tehat a subscribereknek sok esetben eleg lehet az utolso elemet nezni
		}
	}

	private synchronized void notifyCallbacks() {
		for(CFLCallback cb: callbacks) {
			cb.notify(curCFL);
		}
	}

	public synchronized void appendToCFL(int bbId) {
		assert tentativeCFL.size() == curCFL.size(); // azaz ilyenkor nem lehetnek lyukak

		LOG.info("GGG Adding " + bbId + " CFL");

		tentativeCFL.add(bbId);
		curCFL.add(bbId);
		sendElement(new CFLElement(curCFL.size()-1, bbId));
		notifyCallbacks();
	}

	public synchronized void subscribe(CFLCallback cb) {
		LOG.info("GGG CFLCallback subscription");
		callbacks.add(cb);

		// Egyenkent elkuldjuk a notificationt mindegyik eddigirol
		List<Integer> tmpCfl = new ArrayList<>();
		for(Integer x: curCFL) {
			tmpCfl.add(x);
			cb.notify(tmpCfl);
		}
	}
}
