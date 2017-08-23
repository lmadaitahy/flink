/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gg;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class CFLManager {

	protected static final Logger LOG = LoggerFactory.getLogger(CFLManager.class);

	private static boolean logCoord = false;

	public static boolean barrier = false; // barrier between iteration steps

	private static CFLManager sing = null;
	public static CFLManager getSing() {return sing;}

	private static final int port = 4444;

	public static byte tmId = -1;
	public static int numAllSlots = -1;
	public static int numTaskSlotsPerTm = -1;

	public static void create(TaskManager tm, String[] hosts, boolean coordinator) {
		sing = new CFLManager(tm, hosts, coordinator);
	}


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

		cflSendSeqNum = 0;

		jobCounter = 0;

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
	private int numSubscribed = 0;
	private Integer numToSubscribe = null;

	private volatile int cflSendSeqNum = 0;

	// https://stackoverflow.com/questions/6012640/locking-strategies-and-techniques-for-preventing-deadlocks-in-code
	// We obtain multiple locks only in the following orders:
	// When receiving a msg:
	//   CFLManager -> BagOperatorHost -> msgSendLock  or
	//   CFLManager -> msgSendLock  (when the msg triggers sending closeInputBag)
	// When entering through processElement:
	//   BagOperatorHost -> msgSendLock
	private final Object msgSendLock = new Object();

	private volatile int jobCounter = -10;

	public JobID getJobID() {
		return jobID;
	}

	public void setJobID(JobID jobID) {
		LOG.info("CFLManager.setJobID to '" + jobID + "'");
		if (this.jobID != null && !this.jobID.equals(jobID) && jobID != null) {
			throw new RuntimeException("Csak egy job futhat egyszerre. (old: " + this.jobID + ", new: " + jobID + ")");
			// amugy ez akkor is triggerelodik, ha nem CFL-es jobok akar csak egymas utan tobb akarna futni
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
						LOG.info("Connecting sender connection to " + host + ".");
						socket.connect(new InetSocketAddress(host, port), timeout);
						LOG.info("Sender connection connected to  " + host + ".");
						break;
					} catch (SocketTimeoutException exTimeout) {
						LOG.info("Sender connection to            " + host + " timed out, retrying...");
					} catch (ConnectException ex) {
						LOG.info("Sender connection to            " + host + " was refused, retrying...");
						try {
							Thread.sleep(500);
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}
					} catch (IOException e) {
						LOG.info("Sender connection to            " + host + " caused an IOException, retrying... " + e);
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
		LOG.info("All sender connections are up.");
		allSenderUp = true;
	}

	private void sendElement(CFLElement e) {
		for (int i = 0; i<hosts.length; i++) {
			try {
				msgSer.serialize(new Msg(jobCounter, e), senderDataOutputViews[i]);
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
					LOG.info("Listening for incoming connections " + i);
					Socket socket = serverSocket.accept();
					SocketAddress remoteAddr = socket.getRemoteSocketAddress();
					LOG.info("Got incoming connection " + i + " from " + remoteAddr);
					recvRemoteAddresses[i] = remoteAddr;
					connReaders[i] = new ConnReader(socket, i);
					i++;
				}
				LOG.info("All incoming connections connected");
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
			thread.setDaemon(true);
			thread.start();
		}

		@Override
		public void run() {
			try {
				try {
					InputStream ins = socket.getInputStream();
					DataInputViewStreamWrapper divsw = new DataInputViewStreamWrapper(ins);
					while (true) {
						Msg msg = msgSer.deserialize(divsw);
						synchronized (CFLManager.this) {
							if (logCoord) LOG.info("Got " + msg);

							if (msg.jobCounter < jobCounter) {
								// Mondjuk ebbol itt lehet baj, ha vki meg nem kapta meg nem kapta meg a voteStop-hoz eljutashoz szukseges msg-ket.
								// De most van az a sleep(500) a reset elott, remelhetoleg az biztositja, hogy ne jussunk ide.
								LOG.info("Old msg, ignoring (msg.jobCounter = " + msg.jobCounter + ", jobCounter = " + jobCounter + ")");
								continue;
							}
							while (msg.jobCounter > jobCounter) {
								LOG.info("Too new msg, waiting (msg.jobCounter = " + msg.jobCounter + ", jobCounter = " + jobCounter + ")"); // Ilyenkor valojaban a resetre varunk
								Thread.sleep(100);
							}

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
							} else if (msg.subscribeCnt != null) {
								subscribeCntRemote();
							} else if (msg.barrierAllReached != null) {
								barrierAllReachedRemote(msg.barrierAllReached.cflSize);
							} else if (msg.voteStop != null) {
								voteStopRemote();
							} else if (msg.stop != null) {
								stopRemote();
							} else {
								assert false;
							}
						}
					}
				} catch (EOFException e) {
					// This happens when the other TM shuts down. No need to throw a RuntimeException here, as we are shutting down anyway.
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} catch (Throwable e) {
				e.printStackTrace();
				LOG.error(ExceptionUtils.stringifyException(e));
				Runtime.getRuntime().halt(200);
			}
		}
	}

	private synchronized void addTentative(int seqNum, int bbId) {
		while (seqNum >= tentativeCFL.size()) {
			tentativeCFL.add(null);
			// Vigyazat, a kov. if-et nem lehetne max-szal kivaltani, mert akkor osszeakadhatnank
			// az appendToCFL-ben levo inkrementalassal.
			if (seqNum + 1 > cflSendSeqNum) {
				cflSendSeqNum = seqNum + 1;
			}
		}
		assert tentativeCFL.get(seqNum) == null; // (kivenni, ha lesz az UDP-s dolog)
		tentativeCFL.set(seqNum, bbId);

		for (int i = curCFL.size(); i < tentativeCFL.size(); i++) {
			Integer t = tentativeCFL.get(i);
			if (t == null)
				break;
			curCFL.add(t);
			LOG.info("Adding BBID " + t + " to CFL " + System.currentTimeMillis());
			notifyCallbacks();
			// szoval minden elemnel kuldunk kulon, tehat a subscribereknek sok esetben eleg lehet az utolso elemet nezni
		}
	}

	private boolean shouldNotifyTerminalBB() {
		return curCFL.size() > 0 && curCFL.get(curCFL.size() - 1) == terminalBB;
	}

	private synchronized void notifyCallbacks() {
		for (CFLCallback cb: callbacks) {
			cb.notify(curCFL);
		}

		assert callbacks.size() == 0 || terminalBB != -1; // A drivernek be kell allitania a job elindulasa elott. Viszont ebbe a fieldbe a BagOperatorHost.setup-ban kerul.
		if (shouldNotifyTerminalBB()) {
			assert terminalBB != -1;
			// We need to copy, because notifyTerminalBB might call unsubscribe, which would lead to a ConcurrentModificationException
			ArrayList<CFLCallback> origCallbacks = new ArrayList<>(callbacks);
			for (CFLCallback cb: origCallbacks) {
				cb.notifyTerminalBB();
			}
		}
	}


	// --------------------------------------------------------

	// A helyi TM-ben futo operatorok hivjak
	public void appendToCFL(int bbId) {
		synchronized (msgSendLock) {

			// Ugyebar ha valaki appendel a CFL-hez, akkor mindig biztos a dolgaban.
			// (Akkor is, ha tobbet appendel, mert olyan BB-ket is appendel, amelyekben nincs condition node.)
			// Szoval nem fordulhat elo olyan, hogy el akarna agazni a CFL.
			// Emiatt biztosak lehetunk benne, hogy nem akar senki olyankor appendelni, amikor meg nem
			// kapta meg a legutobbi CFL-t. Ebbol kovetkezik, hogy itt nem lehetnek lyukak.
			// Viszont ami miatt ezt megis ki kellett commentezni, az az, hogy szinkronizalasi problema van az
			// addTentative-val olyankor, amikor egymas utan tobb appendToCFL-t hiv valaki.
			//assert tentativeCFL.size() == curCFL.size();

			LOG.info("Adding " + bbId + " to CFL (appendToCFL) " + System.currentTimeMillis());
			sendElement(new CFLElement(cflSendSeqNum++, bbId));
		}
	}

	public synchronized void subscribe(CFLCallback cb) {
		LOG.info("CFLManager.subscribe");
		assert allIncomingUp && allSenderUp;

		// Maybe there could be a waitForReset here

		callbacks.add(cb);

		// Egyenkent elkuldjuk a notificationt mindegyik eddigirol
		List<Integer> tmpCfl = new ArrayList<>();
		for(Integer x: curCFL) {
			tmpCfl.add(x);
			cb.notify(tmpCfl);
		}

		assert terminalBB != -1; // a drivernek be kell allitania a job elindulasa elott
		if (shouldNotifyTerminalBB()) {
			cb.notifyTerminalBB();
		}

		for(CloseInputBag cib: closeInputBags) {
			cb.notifyCloseInput(cib.bagID, cib.opID);
		}

		subscribeCntLocal();
	}

	private void checkVoteStop() {
		if (numToSubscribe != null && numSubscribed == numToSubscribe && callbacks.isEmpty()) {
			voteStopLocal();
		} else {
			if (numToSubscribe == null) {
				LOG.info("checkVoteStop: numToSubscribe == null");
			} else {
				if (callbacks.isEmpty() && numSubscribed != numToSubscribe) {
					LOG.info("checkVoteStop: callbacks.isEmpty(), but numSubscribed != numToSubscribe: " + numSubscribed + " != " + numToSubscribe);
				} else {
					LOG.info("checkVoteStop: callbacks has " + callbacks.size() + " elements");
				}
			}
		}
	}

	public synchronized void unsubscribe(CFLCallback cb) {
		LOG.info("CFLManager.unsubscribe");
		callbacks.remove(cb);

		checkVoteStop();
	}

	private synchronized void reset() {
		LOG.info("Resetting CFLManager.");

		assert callbacks.size() == 0;

		tentativeCFL.clear();
		curCFL.clear();

		cflSendSeqNum = 0;

		bagStatuses.clear();
		bagConsumedStatuses.clear();
		emptyBags.clear();

		closeInputBags.clear();

		barrierReached.clear();

		terminalBB = -1;
		numSubscribed = 0;
		numToSubscribe = null;

		numVoteStops = 0;

		jobCounter++;
	}

	public synchronized void specifyTerminalBB(int bbId) {
		LOG.info("specifyTerminalBB: " + bbId);
		terminalBB = bbId;
	}

	public synchronized void specifyNumToSubscribe(int numToSubscribe) {
		LOG.info("specifyNumToSubscribe: " + numToSubscribe);
		this.numToSubscribe = numToSubscribe;
		checkVoteStop();
	}

    // --------------------------------------------------------


    private static final class BagStatus {

        public int numProduced = 0;
		public boolean produceClosed = false;

		public Set<Integer> producedSubtasks = new HashSet<>();

		public Set<BagID> inputs = new HashSet<>();
		public Set<BagID> inputTo = new HashSet<>();
		public Set<Integer> consumedBy = new HashSet<>();

		public int para = -2;
    }

	private static final class BagConsumptionStatus {

		public int numConsumed = 0;
		public boolean consumeClosed = false;

		public Set<Integer> consumedSubtasks = new HashSet<>();
	}

    private final Map<BagID, BagStatus> bagStatuses = new HashMap<>();

	private final Map<BagIDAndOpID, BagConsumptionStatus> bagConsumedStatuses = new HashMap<>();

	private final Set<BagID> emptyBags = new HashSet<>();

	private final List<CloseInputBag> closeInputBags = new ArrayList<>();

	// -- Begin barrier stuff --
	private final Map<Integer, Integer> barrierReached = new HashMap<>(); // CFLSize -> Int: mely steppel hanyan vegeztek
	private static final Set<Integer> opsInLoop = new HashSet<>(Arrays.asList(15,5,6,7,10,11,16));
	// -- End   barrier stuff --

	private void sendToCoordinator(Msg msg) {
		synchronized (msgSendLock) {
			try {
				msgSer.serialize(msg, senderDataOutputViews[0]);
				senderStreams[0].flush();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void sendToEveryone(Msg msg) {
		synchronized (msgSendLock) {
			for (int i = 0; i < hosts.length; i++) {
				try {
					msgSer.serialize(msg, senderDataOutputViews[i]);
					senderStreams[i].flush();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

    // kliens -> coordinator
    public void consumedLocal(BagID bagID, int numElements, int subtaskIndex, int opID) {
    	sendToCoordinator(new Msg(jobCounter, new Consumed(bagID, numElements, subtaskIndex, opID)));
    }

    private synchronized void consumedRemote(BagID bagID, int numElements, int subtaskIndex, int opID) {
		if (logCoord) LOG.info("consumedRemote(bagID = " + bagID + ", numElements = " + numElements + ", opID = " + opID + ")");

		// Get or init BagStatus
		BagStatus s = bagStatuses.get(bagID);
		if (s == null) {
			s = new BagStatus();
			bagStatuses.put(bagID, s);
		}

		// Get or init BagConsumptionStatus
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
			// Regen azert volt jo itt a -1, mert ilyenkor biztosan nem source. De mostmar nem csak source-nal hasznaljuk a para-t
			assert s.para != -2;
			checkForClosingProduced(b, bagStatuses.get(b), s.para, b.opID);
		}
    }

    private void checkForClosingConsumed(BagID bagID, BagStatus s, BagConsumptionStatus c, int opID) {
		assert !c.consumeClosed;
		if (s.produceClosed) {
			assert c.numConsumed <= s.numProduced; // (ennek belul kell lennie az if-ben mert kivul a reordering miatt nem biztos, hogy igaz)
			if (c.numConsumed == s.numProduced) {
				if (logCoord) LOG.info("checkForClosingConsumed(" + bagID + ", opID = " + opID + "): consumeClosed, because numConsumed = " + c.numConsumed + ", numProduced = " + s.numProduced);
				c.consumeClosed = true;
				closeInputBagLocal(bagID, opID);
			} else {
				if (logCoord) LOG.info("checkForClosingConsumed(" + bagID + ", opID = " + opID + "): needMore, because numConsumed = " + c.numConsumed + ", numProduced = " + s.numProduced);
			}
		}
	}

    // kliens -> coordinator
	public void producedLocal(BagID bagID, BagID[] inpIDs, int numElements, int para, int subtaskIndex, int opID) {
		assert inpIDs.length <= 2; // ha 0, akkor BagSource
		sendToCoordinator(new Msg(jobCounter, new Produced(bagID, inpIDs, numElements, para, subtaskIndex, opID)));
    }

    private synchronized void producedRemote(BagID bagID, BagID[] inpIDs, int numElements, int para, int subtaskIndex, int opID) {
		if (logCoord) LOG.info("producedRemote(bagID = " + bagID + ", numElements = " + numElements + ", opID = " + opID + ")");

		// Get or init BagStatus
		BagStatus s = bagStatuses.get(bagID);
		if (s == null) {
			s = new BagStatus();
			bagStatuses.put(bagID, s);
		}

		assert !s.produceClosed || numElements == 0;

		// Add to s.inputs, and add to the inputTos of the inputs
		for (BagID inp: inpIDs) {
			s.inputs.add(inp);

			BagStatus inpS = bagStatuses.get(inp);
			if (inpS == null) {
				inpS = new BagStatus();
				bagStatuses.put(inp, inpS);
			}
			inpS.inputTo.add(bagID);
		}
		assert s.inputs.size() <= 2;

		// Set para
		s.para = para;

		// Add to s.numProduced
		s.numProduced += numElements;

		// Add to s.producedSubtasks
		assert !s.producedSubtasks.contains(subtaskIndex);
		s.producedSubtasks.add(subtaskIndex);

		checkForClosingProduced(bagID, s, para, opID);

		for (Integer copID: s.consumedBy) {
			checkForClosingConsumed(bagID, s, bagConsumedStatuses.get(new BagIDAndOpID(bagID, copID)), copID);
		}
    }

    private void checkForClosingProduced(BagID bagID, BagStatus s, int para, int opID) {
		if (s.produceClosed) {
			return; // Mondjuk ezt nem ertem, hogy hogy fordulhat elo
		}

		if (s.inputs.size() == 0) {
			// source, tehat mindenhonnan varunk
			assert para != -1;
			int totalProducedMsgs = s.producedSubtasks.size();
			assert totalProducedMsgs <= para;
			if (totalProducedMsgs == para) {
				if (logCoord) LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + "): produceClosed");
				s.produceClosed = true;
			}
		} else {

			// Az isEmpty produced-janak lezarasa vegett van most ez bent. Annak ugyebar 1 a para-ja, es onnan fog is kuldeni.
			// (Itt lehetne kicsit szebben, hogy ne legyen kodduplikalas a fentebbi resszel.)
			int totalProducedMsgs = s.producedSubtasks.size();
			assert totalProducedMsgs <= para;
			if (totalProducedMsgs == para) {
				if (logCoord) LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + "): produceClosed, because totalProducedMsgs == para");
				s.produceClosed = true;
			}

			if (!s.produceClosed) {
				boolean needMore = false;
				// Ebbe rakjuk ossze az inputok consumedSubtasks-jait
				Set<Integer> needProduced = new HashSet<>();
				for (BagID inp : s.inputs) {
					if (emptyBags.contains(inp)) {
						// enelkul olyankor lenne gond, ha egy binaris operator egyik inputja ures, emiatt a closeInputBag
						// mindegyik instance-t megloki, viszont a checkForClosingProduced csak a masik input alapjan nezi,
						// hogy honnan kell jonni, es ezert nem szamit bizonyos jovesekre
						for (int i=0; i< para; i++) {
							needProduced.add(i);
						}
					}
					BagConsumptionStatus bcs = bagConsumedStatuses.get(new BagIDAndOpID(inp, opID));
					if (bcs != null) {
						if (!bcs.consumeClosed) {
							if (logCoord)
								LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + "): needMore, because !bcs.consumeClosed");
							needMore = true;
							break;
						}
						needProduced.addAll(bcs.consumedSubtasks);
					} else {
						// Maybe we run into trouble here if this happens because we have a binary operator that sends produced while it haven't yet consumed from one of its inputs.
						// Hm, but actually I don't think I have such an operator at the moment.   Ez kozben mar nem ervenyes
						// But this can actually happen with the nonEmpty stuff. But then we just close produced here.
						// Vagy esetleg az is lehet, hogy azert kerultunk ide mert nem a termeszetes sorrendben jott a produced es a consumed?
						if (logCoord)
							LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + "): bcs == null");
					}
				}
				if (!needMore && !s.produceClosed) {
					int needed = needProduced.size();
					int actual = s.producedSubtasks.size();
					assert actual <= needed || needed == 0; // This should be true, because we have already checked consumeClose above. (needed == 0 when responding to empty input bags)
					if (actual < needed) {
						if (logCoord)
							LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + "): needMore, because actual = " + actual + ", needed = " + needed);
						needMore = true;
					}
				}
				if (!needMore && !s.produceClosed) {
					if (logCoord)
						LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + "): produceClosed");
					s.produceClosed = true;
				}
			}
		}

		if (s.produceClosed) {
			if (s.numProduced == 0) {
				if (logCoord) LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + ") detected an empty bag");
				emptyBags.add(bagID);
				closeInputBagLocal(bagID, CloseInputBag.emptyBag);
			}

			if (barrier) {
				if (opsInLoop.contains(opID)) {
					Integer oldVal = barrierReached.get(bagID.cflSize);
					if (oldVal == null) {
						barrierReached.put(bagID.cflSize,1);
					} else {
						barrierReached.put(bagID.cflSize, oldVal + 1);
					}
					int newVal = barrierReached.get(bagID.cflSize);
					if (logCoord) LOG.info("checkForClosingProduced(" + bagID + ", " + s + ", opID = " + opID + ") barrierReached cflSize=" + bagID.cflSize + ", newVal=" + newVal + ", opsInLoop.size==" + opsInLoop.size());
					if (newVal == opsInLoop.size()) { // (a +1 amiatt kell, mert a MutableBag ketszer kell)
						barrierAllReachedLocal(bagID.cflSize);
					}
				}
			}
		}
	}

    // A coordinator a local itt. Az operatorok inputjainak a close-olasat valtja ez ki.
    private synchronized void closeInputBagLocal(BagID bagID, int opID) {
		assert coordinator;
		sendToEveryone(new Msg(jobCounter, new CloseInputBag(bagID, opID)));
    }

	// (runs on client)
    private synchronized void closeInputBagRemote(BagID bagID, int opID) {
		if (logCoord) LOG.info("closeInputBagRemote(" + bagID + ", " + opID +")");

		closeInputBags.add(new CloseInputBag(bagID, opID));

		ArrayList<CFLCallback> origCallbacks = new ArrayList<>(callbacks);
		for (CFLCallback cb: origCallbacks) {
			cb.notifyCloseInput(bagID, opID);
		}
    }


    private void subscribeCntLocal() {
		sendToEveryone(new Msg(jobCounter, new SubscribeCnt()));
	}

	private synchronized void subscribeCntRemote() {
		numSubscribed++;
		checkVoteStop();
	}

	private synchronized void barrierAllReachedLocal(int cflSize) {
		assert coordinator;
		sendToEveryone(new Msg(jobCounter, new BarrierAllReached(cflSize)));
	}

	private synchronized void barrierAllReachedRemote(int cflSize) {
		if (logCoord) LOG.info("barrierAllReachedRemote(" + cflSize + ")");

		ArrayList<CFLCallback> origCallbacks = new ArrayList<>(callbacks);
		for (CFLCallback cb: origCallbacks) {
			cb.notifyBarrierAllReached(cflSize);
		}
	}

	private synchronized void voteStopLocal() {
		LOG.info("voteStopLocal()");
		sendToCoordinator(new Msg(jobCounter, new VoteStop()));
	}

	private int numVoteStops = 0;

	private synchronized void voteStopRemote() {
		LOG.info("voteStopRemote()");
		assert coordinator;
    	numVoteStops++;
    	if (numVoteStops == hosts.length) {
    		stopLocal();
		}
	}

	private synchronized void stopLocal() {
		LOG.info("stopLocal()");
		assert coordinator;
		sendToEveryone(new Msg(jobCounter, new Stop()));
	}

	private synchronized void stopRemote() {
		LOG.info("stopRemote()");
		LOG.info("tm.CFLVoteStop();");
		tm.CFLVoteStop();
		setJobID(null);
		reset();
	}

    // --------------------------------

    public static class Msg {

		public int jobCounter;

		// These are nullable, and exactly one should be non-null
		public CFLElement cflElement;
		public Consumed consumed;
		public Produced produced;
		public CloseInputBag closeInputBag;
		public SubscribeCnt subscribeCnt;
		public BarrierAllReached barrierAllReached;
		public VoteStop voteStop;
		public Stop stop;

		public Msg() {}

		public Msg(int jobCounter, CFLElement cflElement) {
			this.jobCounter = jobCounter;
			this.cflElement = cflElement;
		}

		public Msg(int jobCounter, Consumed consumed) {
			this.jobCounter = jobCounter;
			this.consumed = consumed;
		}

		public Msg(int jobCounter, Produced produced) {
			this.jobCounter = jobCounter;
			this.produced = produced;
		}

		public Msg(int jobCounter, CloseInputBag closeInputBag) {
			this.jobCounter = jobCounter;
			this.closeInputBag = closeInputBag;
		}

		public Msg(int jobCounter, SubscribeCnt subscribeCnt) {
			this.jobCounter = jobCounter;
			this.subscribeCnt = subscribeCnt;
		}

		public Msg(int jobCounter, BarrierAllReached barrierAllReached) {
			this.jobCounter = jobCounter;
			this.barrierAllReached = barrierAllReached;
		}

		public Msg(int jobCounter, VoteStop voteStop) {
			this.jobCounter = jobCounter;
			this.voteStop = voteStop;
		}

		public Msg(int jobCounter, Stop stop) {
			this.jobCounter = jobCounter;
			this.stop = stop;
		}

		@Override
		public String toString() {
			return "Msg{" +
					"jobCounter=" + jobCounter +
					", cflElement=" + cflElement +
					", consumed=" + consumed +
					", produced=" + produced +
					", closeInputBag=" + closeInputBag +
					", subscribeCnt=" + subscribeCnt +
					", barrierAllReached=" + barrierAllReached +
					", voteStop=" + voteStop +
					", stop=" + stop +
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

		// Used in the opID field, when the bag is empty. In this case, all operators consuming this bag will close it.
		// They will also send produced(0), if they are marked with EmptyFromEmpty.
		public final static int emptyBag = -100;

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

	public static class SubscribeCnt {
		public byte dummy; // To make it a POJO
	}

	public static class BarrierAllReached {

		public int cflSize;

		public BarrierAllReached() {}

		public BarrierAllReached(int cflSize) {
			this.cflSize = cflSize;
		}

		@Override
		public String toString() {
			return "BarrierAllReached{" +
					"cflSize=" + cflSize +
					'}';
		}
	}

	public static class VoteStop {
		public byte dummy; // To make it a POJO
	}

	public static class Stop {
		public byte dummy; // To make it a POJO
	}

	// --------------------------------
}
