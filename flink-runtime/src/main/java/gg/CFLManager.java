package gg;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.*;

public class CFLManager {

	protected static final Logger LOG = LoggerFactory.getLogger(CFLManager.class);

	private static CFLManager sing = null;
	public static CFLManager getSing() {return sing;}

	private static final int port = 4444;

	public static byte tmId = -1;
	public static int numAllSlots = -1;
	public static int numTaskSlotsPerTm = -1;

//	public static void create(TaskManager tm) {
//		sing = new CFLManager(tm);
//	}

	public static void create(TaskManager tm, String[] hosts, boolean coordinator) {
		sing = new CFLManager(tm, hosts, coordinator);
	}


//	public CFLManager(TaskManager tm) {
//		// local execution
//		this(tm, new String[]{}, true);
//	}

	public CFLManager(TaskManager tm, String[] hosts, boolean coordinator) {
		this.tm = tm;
		this.hosts = hosts;
		this.coordinator = coordinator;
		connReaders = new ConnReader[hosts.length];
		recvRemoteAddresses = new SocketAddress[hosts.length];

		connAccepter = new ConnAccepter(); //thread

		senderSockets = new Socket[hosts.length];
		senderStreams = new OutputStream[hosts.length];
		senderDataOutputViews = new DataOutputViewStreamWrapper[hosts.length];

		createSenderConnections();
	}

	private TaskManager tm;

	private boolean coordinator;

	private String[] hosts;
	private ConnAccepter connAccepter;
	private ConnReader[] connReaders;
	private SocketAddress[] recvRemoteAddresses;

	private Socket[] senderSockets;
	private OutputStream[] senderStreams;
	private DataOutputViewStreamWrapper[] senderDataOutputViews;

	private volatile boolean allSenderUp = false;
	private volatile boolean allIncomingUp = false;

	private List<Integer> tentativeCFL = new ArrayList<>(); // ez lehet lyukas, ha nem sorrendben erkeznek meg az elemek
	private List<Integer> curCFL = new ArrayList<>(); // ez sosem lyukas

	private List<CFLCallback> callbacks = new ArrayList<>();

	private int terminalBB = -1;

	private int cflSendSeqNum = -1000000;

	public JobID getJobID() {
		return jobID;
	}

	public void setJobID(JobID jobID) {
		LOG.info("GGG CFLManager.setJobID to '" + jobID + "'");
		if (this.jobID != null && !this.jobID.equals(jobID) && jobID != null) {
			throw new RuntimeException("GGG Csak egy job futhat egyszerre. (old: " + this.jobID + ", new: " + jobID + ")");
		}
		this.jobID = jobID;
	}

	private JobID jobID = null;

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
				senderDataOutputViews[i] = new DataOutputViewStreamWrapper(senderStreams[i]);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			i++;
		}
		LOG.info("GGG All sender connections are up.");
		allSenderUp = true;
	}

	private void sendElement(CFLElement e) {
		for (int i = 0; i<hosts.length; i++) {
			try {
				msgSer.serialize(new Msg(e), senderDataOutputViews[i]);
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
				allIncomingUp = true;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private class ConnReader implements Runnable {

		Thread thread;

		Socket socket;

		int connID;

		public ConnReader(Socket socket, int connID) {
			this.socket = socket;
			this.connID = connID;
			thread = new Thread(this, "ConnReader_" + connID);
			thread.start();
		}

		@Override
		public void run() {
			try {
				InputStream ins = socket.getInputStream();
				DataInputViewStreamWrapper divsw = new DataInputViewStreamWrapper(ins);
				while (true) {
					Msg msg = msgSer.deserialize(divsw);
					LOG.info("GGG Got " + msg);
					if (msg.cflElement != null) {
						addTentative(msg.cflElement.seqNum, msg.cflElement.bbId); // will do the callbacks
					} else if (msg.consumed != null) {
						assert coordinator;
						consumedRemote(msg.consumed.bagID, msg.consumed.numElements, msg.consumed.subtaskIndex, msg.consumed.opID);
					} else if (msg.produced != null) {
						assert coordinator;
						producedRemote(msg.produced.bagID, msg.produced.inpIDs, msg.produced.numElements, msg.produced.para, msg.produced.subtaskIndex, msg.produced.opID);
					} else if (msg.closeInputBag != null) {
						closeInputBagRemote(msg.closeInputBag.bagID, msg.closeInputBag.opID);
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private synchronized void addTentative(int seqNum, int bbId) {
		while (seqNum >= tentativeCFL.size()) {
			tentativeCFL.add(null);
			cflSendSeqNum = Math.max(cflSendSeqNum, seqNum + 1);
		}
		assert tentativeCFL.get(seqNum) == null; // (kivenni, ha lesz az UDP-s dolog)
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
		for (CFLCallback cb: callbacks) {
			cb.notify(curCFL);
		}

		assert terminalBB != -1; // a drivernek be kell allitania a job elindulasa elott
		if (curCFL.get(curCFL.size() - 1) == terminalBB) {
			// We need to copy, because notifyTerminalBB might call unsubscribe, which would lead to a ConcurrentModificationException
			ArrayList<CFLCallback> origCallbacks = new ArrayList<>(callbacks);
			for (CFLCallback cb: origCallbacks) {
				cb.notifyTerminalBB();
			}
		}
	}


	// --------------------------------------------------------

	// A helyi TM-ben futo operatorok hivjak
	public synchronized void appendToCFL(int bbId) {
		assert tentativeCFL.size() == curCFL.size(); // azaz ilyenkor nem lehetnek lyukak

		LOG.info("GGG Adding " + bbId + " to CFL (appendToCFL)");

		//tentativeCFL.add(bbId);
		//curCFL.add(bbId);
		//sendElement(new CFLElement(curCFL.size()-1, bbId));
		//notifyCallbacks();

		sendElement(new CFLElement(cflSendSeqNum++, bbId));
	}

	public synchronized void subscribe(CFLCallback cb) {
		LOG.info("GGG CFLManager.subscribe");
		assert allIncomingUp && allSenderUp;

		callbacks.add(cb);

		// Egyenkent elkuldjuk a notificationt mindegyik eddigirol
		List<Integer> tmpCfl = new ArrayList<>();
		for(Integer x: curCFL) {
			tmpCfl.add(x);
			cb.notify(tmpCfl);
		}

		assert terminalBB != -1; // a drivernek be kell allitania a job elindulasa elott
		if (curCFL.size() > 0 && curCFL.get(curCFL.size() - 1) == terminalBB) {
			cb.notifyTerminalBB();
		}
	}

	public synchronized void unsubscribe(CFLCallback cb) {
		LOG.info("GGG CFLManager.unsubscribe");
		callbacks.remove(cb);

		// Arra kene vigyazni, hogy nehogy az legyen, hogy olyankor hiszi azt, hogy mindenki unsubscribe-olt, amikor meg nem mindenki subscribe-olt.
		// Egyelore figyelmen kivul hagyom ezt a problemat, valszeg nem nagyon fogok belefutni.
		if (callbacks.isEmpty()) {
			tm.CFLVoteStop();
			setJobID(null);
		}
	}

	public synchronized void resetCFL() {
		LOG.info("GGG Resetting CFL.");

		tentativeCFL.clear();
		curCFL.clear();

		cflSendSeqNum = 0;

		bagStatuses.clear();
		bagConsumedStatuses.clear();
	}

	public synchronized void specifyTerminalBB(int bbId) {
		LOG.info("GGG specifyTerminalBB: " + bbId);
		terminalBB = bbId;
	}

    // --------------------------------------------------------


    private static final class BagStatus {

        public int numProduced = 0;
		public boolean produceClosed = false;

		public Set<Integer> producedSubtasks = new HashSet<>();

		public Set<BagID> inputs = new HashSet<>();
		public Set<BagID> inputTo = new HashSet<>();
		public Set<Integer> consumedBy = new HashSet<>();
    }

	private static final class BagConsumptionStatus {

		public int numConsumed = 0;
		public boolean consumeClosed = false;

		public Set<Integer> consumedSubtasks = new HashSet<>();
	}

    private final Map<BagID, BagStatus> bagStatuses = new HashMap<>();

	private final Map<BagIDAndOpID, BagConsumptionStatus> bagConsumedStatuses = new HashMap<>();

    // kliens -> coordinator
    public synchronized void consumedLocal(BagID bagID, int numElements, int subtaskIndex, int opID) {
//		if (coordinator) {
//			consumedRemote(bagID, numElements, subtaskIndex, opID);
//		} else {
			try {
				msgSer.serialize(new Msg(new Consumed(bagID, numElements, subtaskIndex, opID)), senderDataOutputViews[0]);
				senderStreams[0].flush();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
//		}
    }

    private synchronized void consumedRemote(BagID bagID, int numElements, int subtaskIndex, int opID) {
		LOG.info("consumedRemote(bagID = " + bagID + ", numElements = " + numElements + ", opID = " + opID + ")");

    	BagStatus s = bagStatuses.get(bagID);
		BagIDAndOpID key = new BagIDAndOpID(bagID, opID);
		BagConsumptionStatus c = bagConsumedStatuses.get(key);
		if (c == null) {
			c = new BagConsumptionStatus();
			bagConsumedStatuses.put(key, c);
		}

		assert !c.consumeClosed;

		s.consumedBy.add(opID);

		c.consumedSubtasks.add(subtaskIndex);

		c.numConsumed += numElements;

		checkForClosingConsumed(bagID, s, c, opID);

		for (BagID b: bagStatuses.get(bagID).inputTo) {
			// azert jo itt a -1, mert ilyenkor biztosan nem source
			checkForClosingProduced(bagStatuses.get(b), -1, opID);
		}
    }

    private void checkForClosingConsumed(BagID bagID, BagStatus s, BagConsumptionStatus c, int opID) {
		if (s.produceClosed) {
			LOG.info("checkForClosingConsumed(" + bagID + ", opID = " + opID + "): numConsumed = " + c.numConsumed + ", numProduced = " + s.numProduced);
			assert c.numConsumed <= s.numProduced; // (ennek belul kell lennie az if-ben mert kivul a reordering miatt nem biztos, hogy igaz)
			if (c.numConsumed == s.numProduced) {
				c.consumeClosed = true;
				closeInputBagLocal(bagID, opID);
			}
		}
	}

    // kliens -> coordinator
	public synchronized void producedLocal(BagID bagID, BagID[] inpIDs, int numElements, int para, int subtaskIndex, int opID) {
		assert inpIDs.length <= 2; // ha 0, akkor BagSource

//		if (coordinator) {
//			producedRemote(bagID, inpIDs, numElements, para, subtaskIndex, opID);
//		} else {
			try {
				msgSer.serialize(new Msg(new Produced(bagID, inpIDs, numElements, para, subtaskIndex, opID)), senderDataOutputViews[0]);
				senderStreams[0].flush();
			} catch (IOException e) {
				throw new RuntimeException();
			}
//		}
    }

    private synchronized void producedRemote(BagID bagID, BagID[] inpIDs, int numElements, int para, int subtaskIndex, int opID) {
    	LOG.info("producedRemote(bagID = " + bagID + ", numElements = " + numElements + ", opID = " + opID + ")");

		// Get or init BagStatus
		BagStatus s = bagStatuses.get(bagID);
		if (s == null) {
			s = new BagStatus();
			bagStatuses.put(bagID, s);
		}

		assert !s.produceClosed;

		// Add to s.inputs, and add to the inputTos of the inputs
		for (BagID inp: inpIDs) {
			s.inputs.add(inp);
			bagStatuses.get(inp).inputTo.add(bagID);
		}
		assert s.inputs.size() <= 2;

		// Add to s.numProduced
		s.numProduced += numElements;

		// Add to s.producedSubtasks
		assert !s.producedSubtasks.contains(subtaskIndex);
		s.producedSubtasks.add(subtaskIndex);

		checkForClosingProduced(s, para, opID);

		for (Integer copID: s.consumedBy) {
			checkForClosingConsumed(bagID, s, bagConsumedStatuses.get(new BagIDAndOpID(bagID, copID)), copID);
		}
    }

    private void checkForClosingProduced(BagStatus s, int para, int opID) {
		if (s.inputs.size() == 0) {
			// source, tehat mindenhonnan varunk
			assert para != -1;
			int totalProducedMsgs = s.producedSubtasks.size();
			assert totalProducedMsgs <= para;
			if (totalProducedMsgs == para) {
				s.produceClosed = true;
			}
		} else {
			boolean needMore = false;
			// Ebbe rakjuk ossze az inputok consumedSubtasks-jait
			Set<Integer> needProduced = new HashSet<>();
			for (BagID inp: s.inputs) {
				if (!bagConsumedStatuses.get(new BagIDAndOpID(inp, opID)).consumeClosed) {
					needMore = true;
					break;
				}
				needProduced.addAll(bagConsumedStatuses.get(new BagIDAndOpID(inp, opID)).consumedSubtasks);
			}
			if (!needMore) {
				int needed = needProduced.size();
				int actual = s.producedSubtasks.size();
				LOG.info("checkForClosingProduced(" + s + ", opID = " + opID + "): actual = " + actual + ", needed = " + needed);
				assert actual <= needed; // This should be true, because we already checked consumeClose above
				if (actual < needed) {
					needMore = true;
				}
			}
			if (!needMore) {
				s.produceClosed = true;
			}
		}
	}

    // A coordinator a local itt. Az operatorok inputjainak a close-olasat valtja ez ki.
    private synchronized void closeInputBagLocal(BagID bagID, int opID) {
		assert coordinator;

		//closeInputBagRemote(bagID, opID);

		for (int i = 0; i<hosts.length; i++) {
			try {
				msgSer.serialize(new Msg(new CloseInputBag(bagID, opID)), senderDataOutputViews[i]);
				senderStreams[i].flush();
			} catch (IOException e1) {
				throw new RuntimeException(e1);
			}
		}
    }

	// (runs on client)
    private synchronized void closeInputBagRemote(BagID bagID, int opID) {
		LOG.info("closeInputBagRemote(" + bagID + ", " + opID +")");

		ArrayList<CFLCallback> origCallbacks = new ArrayList<>(callbacks);
		for (CFLCallback cb: origCallbacks) {
			cb.notifyCloseInput(bagID, opID);
		}
    }

    // --------------------------------

    public static class Msg {

		// These are nullable, and exactly one should be non-null
		public CFLElement cflElement;
		public Consumed consumed;
		public Produced produced;
		public CloseInputBag closeInputBag;

		public Msg() {}

		public Msg(CFLElement cflElement) {
			this.cflElement = cflElement;
		}

		public Msg(Consumed consumed) {
			this.consumed = consumed;
		}

		public Msg(Produced produced) {
			this.produced = produced;
		}

		public Msg(CloseInputBag closeInputBag) {
			this.closeInputBag = closeInputBag;
		}

		@Override
		public String toString() {
			return "Msg{" +
					"cflElement=" + cflElement +
					", consumed=" + consumed +
					", produced=" + produced +
					", closeInputBag=" + closeInputBag +
					'}';
		}
	}

	private static final TypeSerializer<Msg> msgSer = TypeInformation.of(Msg.class).createSerializer(new ExecutionConfig());

	public static class Consumed {

		public BagID bagID;
		public int numElements;
		public int subtaskIndex;
		public int opID;

		public Consumed() {}

		public Consumed(BagID bagID, int numElements, int subtaskIndex, int opID) {
			this.bagID = bagID;
			this.numElements = numElements;
			this.subtaskIndex = subtaskIndex;
			this.opID = opID;
		}

		@Override
		public String toString() {
			return "Consumed{" +
					"bagID=" + bagID +
					", numElements=" + numElements +
					", subtaskIndex=" + subtaskIndex +
					", opID=" + opID +
					'}';
		}
	}

	public static class Produced {

		public BagID bagID;
		public BagID[] inpIDs;
		public int numElements;
		public int para;
		public int subtaskIndex;
		public int opID;

		public Produced() {}

		public Produced(BagID bagID, BagID[] inpIDs, int numElements, int para, int subtaskIndex, int opID) {
			this.bagID = bagID;
			this.inpIDs = inpIDs;
			this.numElements = numElements;
			this.para = para;
			this.subtaskIndex = subtaskIndex;
			this.opID = opID;
		}

		@Override
		public String toString() {
			return "Produced{" +
					"bagID=" + bagID +
					", inpIDs=" + Arrays.toString(inpIDs) +
					", numElements=" + numElements +
					", para=" + para +
					", subtaskIndex=" + subtaskIndex +
					", opID=" + opID +
					'}';
		}
	}

	public static class CloseInputBag {

		public BagID bagID;
		public int opID;

		public CloseInputBag() {}

		public CloseInputBag(BagID bagID, int opID) {
			this.bagID = bagID;
			this.opID = opID;
		}

		@Override
		public String toString() {
			return "CloseInputBag{" +
					"bagID=" + bagID +
					", opID=" + opID +
					'}';
		}
	}

	// --------------------------------
}
