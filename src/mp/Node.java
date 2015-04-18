package mp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Iterator;

public class Node implements Runnable {

	private int myId;
	public List<Finger> fingers;
	public int pred; // predecessor
	public List<Integer> keys;
	private AtomicBoolean running;
	private Manager mngr;
	private Socket clientSocket;

	public static final int FINGER_LENGTH = 4;

	private ServerSocket serverSocket;

	/**
	 * Used for first node.
	 * 
	 * @param id
	 * @param keys
	 */
	public Node(int id, Manager mngr, int port) {
		this.myId = id;
		this.keys = new ArrayList<Integer>();
		for (int i = 0; i < 256; i++)
			keys.add(i);

		fingers = new ArrayList<Finger>();

		for (int i = 1; i <= Node.FINGER_LENGTH; i++) {
			int start = (int) (id + Math.pow(2, i - 1)
					% Math.pow(2, Node.FINGER_LENGTH));
			int nStart = (int) (id + Math.pow(2, i)
					% Math.pow(2, Node.FINGER_LENGTH));
			Finger f = new Finger(start, new int[] { start, nStart }, 0);
			fingers.add(f);
		}

		pred = 0;

		this.mngr = mngr;
		// queue = new LinkedBlockingQueue<Message>();
		running = new AtomicBoolean();
		running.set(true);
		bind(port);
	}

	public Node(int id, int nPrime, Manager mngr, int port) {
		this.myId = id;
		this.keys = new ArrayList<Integer>();

		fingers = new ArrayList<Finger>();
		for (int i = 1; i <= Node.FINGER_LENGTH; i++) {
			int start = (int) (id + Math.pow(2, i - 1)
					% Math.pow(2, Node.FINGER_LENGTH));
			int nStart = (int) (id + Math.pow(2, i)
					% Math.pow(2, Node.FINGER_LENGTH));
			Finger f = new Finger(start, new int[] { start, nStart });
			fingers.add(f);
		}

		pred = -1;

		this.mngr = mngr;
		running = new AtomicBoolean();
		running.set(true);

		bind(port);

		initFingers(nPrime);
		updateOthers(myId);
		moveKeys(pred, myId);

	}

	@Override
	public void run() {
		while (running.get()) {

			String input = null;
			try {
				clientSocket = serverSocket.accept();
				PrintWriter out = new PrintWriter(
						clientSocket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(
						clientSocket.getInputStream()));
				if ((input = in.readLine()).compareTo(Manager.STOP) == 0)
					break;

				Message msg = new Message(input);

				if (msg.getType() == Message.FIND_SUCCESSOR) {
					out.println(findSuccessor(myId, msg.getId()));
				} else if (msg.getType() == Message.GET_SUCCESSOR) {
					out.println(getSuccessor(msg.getN()));
				} else if (msg.getType() == Message.GET_PREDECESSOR) {
					out.println(getPredecessor(msg.getN()));
				} else if (msg.getType() == Message.SET_PREDECESSOR) {
					setPredecessor(msg.getN(), msg.getId());
				} else if (msg.getType() == Message.CLOSEST_PRECEDING) {
					out.println(closestPrecedingFinger(msg.getN(), msg.getId()));
				} else if (msg.getType() == Message.UPDATE_FINGERS) {
					updateFingers(msg.getN(), msg.getId(), msg.getNPrime());
				} else if (msg.getType() == Message.UPDATE_SUCCESSOR) {
					if (msg.getN() == myId && msg.getN() != msg.getId()) {
						for (int i = 0; i < Node.FINGER_LENGTH; i++) {
							if (inBetweenBB(new int[] {
									fingers.get(i).getStart(),
									fingers.get(i).getSuccessor() },
									msg.getId())) {
								fingers.get(i).setSuccessor(msg.getId());
							}
							updateSuccessor(fingers.get(i).getStart(),
									msg.getId());

						}
						updateSuccessor(fingers.get(0).getSuccessor(),
								msg.getId());
					}
				} else if (msg.getType() == Message.REMOVE_NODE) {
					int nPrime = msg.getNPrime();			
					if (msg.getId() != myId && msg.getN() == myId) {

						for (int i = 0; i < Node.FINGER_LENGTH; i++) {
							if (fingers.get(i).getSuccessor() == msg.getId()) {
								fingers.get(i).setSuccessor(msg.getNPrime());
							}
							
						}

						if(pred == msg.getId())
							pred = findPredecessor(getSuccessor(msg.getNPrime()), msg.getN());
						
						for(int i = 0; inBetweenPB(new int[]{myId, msg.getId()}, i); i++) {
							this.keys.add(i);
						}
						
					}
				}else if(msg.getType() == Message.MOVE_KEY) {
					if(msg.getN() == myId) {
						Iterator<Integer> iter = this.keys.iterator();
						String keysToSend = "";
						while(iter.hasNext()) {
							int k = iter.next();
							if(inBetweenPB(new int[]{myId, msg.getId()}, k)) {
								keysToSend += k + " ";
								iter.remove();
							}
						}
						out.println(keysToSend);
					}
				}
				else if(msg.getType() == Message.MOVE_KEY_DELETE) {
					this.keys.addAll(msg.getKey());
					Collections.sort(this.keys);
				}

				clientSocket.close();
			} catch (IOException e) {
				// e.printStackTrace();
			}
		}

	}
	
	public void moveKeys(int predecessor, int id) {
		if(predecessor != myId)
			try {
				Socket sock = new Socket(Manager.HOST, mngr.getNodeAddress(predecessor));
				PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(
						sock.getInputStream()));
	
				Message msg = new Message(Message.MOVE_KEY, predecessor, id);
				out.println(msg.toString());
	
				String resp = in.readLine();
				String[] keyString = resp.split(" ");
	//			if(!isParsable(keyString[0])) {
	//				moveKeys(0, id);
	//			}
				if(isParsable(keyString[0])){
					for(String k : keyString)
						this.keys.add(Integer.valueOf(k));
				}
				else{
					moveKeys(findSuccessor(id, fingers.get(0).getStart()), id);
				}
				sock.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public static boolean isParsable(String input) {
		boolean parsable = true;
		try {
			Integer.parseInt(input);
		} catch(NumberFormatException e) {
			parsable = false;
		}
		return parsable;
	}

	public void initFingers(int nPrime) {
		fingers.get(0).setSuccessor(
				findSuccessor(nPrime, fingers.get(0).getStart()));
		pred = getPredecessor(fingers.get(0).getSuccessor());
		setPredecessor(fingers.get(0).getSuccessor(), myId);

		for (int i = 0; i < Node.FINGER_LENGTH - 1; i++) {
			int a = myId;
			int b = fingers.get(i).getSuccessor();
			int c = fingers.get(i + 1).getStart();
			if (inBetweenBP(new int[] { a, b }, c)) {
				fingers.get(i + 1).setSuccessor(fingers.get(i).getSuccessor());
			} else {
				fingers.get(i + 1).setSuccessor(
						findSuccessor(nPrime, fingers.get(i + 1).getStart()));
			}
		}
	}

	public int findSuccessor(int n, int id) {
		if (n == myId) {
			if (inBetweenPB(new int[] { n, getSuccessor(n) }, id))
				return getSuccessor(n);

			if (n == id)
				return n;

			return findSuccessor(getSuccessor(n), id);
		} else {
			try {
				int addr;
				
				if((addr= mngr.getNodeAddress(n)) == -1) return -1;
				Socket sock = new Socket(Manager.HOST, addr);
				PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(
						sock.getInputStream()));

				Message msg = new Message(Message.FIND_SUCCESSOR, n, id);
				out.println(msg.toString());

				String resp = in.readLine();
				sock.close();

				return Integer.parseInt(resp);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return -1;

	}

	public int findPredecessor(int n, int id) {
		int nPrime = n;
		int succ = getSuccessor(nPrime);
		while (!inBetweenPB(new int[] { nPrime, succ }, id)) {
			nPrime = closestPrecedingFinger(nPrime, id);
		}

		return nPrime;

	}

	/**
	 * Closest finger preceding id
	 * 
	 * @param n
	 * @param id
	 * @return
	 */
	public int closestPrecedingFinger(int n, int id) {
		if (n == myId) {
			for (int i = Node.FINGER_LENGTH - 1; i >= 0; i--) {
				int s = fingers.get(i).getSuccessor();
				if (inBetweenBP(new int[] { n, id }, s) && s != id)
					return fingers.get(i).getSuccessor();
			}
		} else {
			try {
				Socket sock = new Socket(Manager.HOST, mngr.getNodeAddress(n));
				PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(
						sock.getInputStream()));

				Message msg = new Message(Message.CLOSEST_PRECEDING, id);
				out.println(msg.toString());

				String resp = in.readLine();
				sock.close();

				return Integer.parseInt(resp);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return myId;
	}

	public void updateOthers(int n) {
		if (n == myId) {
			updateSuccessor(getSuccessor(n), n);
		}

	}

	public void updateSuccessor(int n, int id) {
		try {
			int addr;

			if ((addr = mngr.getNodeAddress(n)) == -1)
				return;
			Socket sock = new Socket(Manager.HOST, mngr.getNodeAddress(n));
			PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(
					sock.getInputStream()));

			Message msg = new Message(Message.UPDATE_SUCCESSOR, n, id);
			out.println(msg.toString());

			sock.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void removeNode(int n, int id, int nPrime, int count) {
		if (n != myId) {
			if(count == 0)
				moveKeysDelete(id, nPrime);
			try {
				int addr;
				if((addr = mngr.getNodeAddress(n)) == -1) return;
				Socket sock = new Socket(Manager.HOST, addr);
				PrintWriter out = new PrintWriter(sock.getOutputStream(), true);

				Message msg = new Message(Message.REMOVE_NODE, n, id, nPrime);
				out.println(msg.toString());

				sock.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void moveKeysDelete(int id, int successor) {
			try {
				Socket sock = new Socket(Manager.HOST, mngr.getNodeAddress(successor));
				PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(
						sock.getInputStream()));
				
				Message msg = new Message(Message.MOVE_KEY_DELETE, successor, id, 0, this.keys);
				out.println(msg.toString());

				sock.close();
	
			} catch (IOException e) {
				e.printStackTrace();
			}
			
	}

	public void updateFingers(int n, int s, int i) {
		if (n == myId) {
			if (inBetweenBP(new int[] { n, fingers.get(i).getSuccessor() }, s)) {
				fingers.get(i).setSuccessor(s);
				int p = findPredecessor(myId, myId);
				if (p != s)
					updateFingers(p, s, i);
			}
		} else {
			try {
				Socket sock = new Socket(Manager.HOST, mngr.getNodeAddress(n));
				PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(
						sock.getInputStream()));

				Message msg = new Message(Message.UPDATE_FINGERS, n, s, i);
				out.println(msg.toString());

				sock.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	} 

	public int getSuccessor(int n) {
		if (n == myId)
			return fingers.get(0).getSuccessor();

		try {
			Socket sock = new Socket(Manager.HOST, mngr.getNodeAddress(n));
			PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(
					sock.getInputStream()));

			Message msg = new Message(Message.GET_SUCCESSOR, n);
			out.println(msg.toString());

			String resp = in.readLine();
			sock.close();

			return Integer.parseInt(resp);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return n;
	}

	public int getPredecessor(int n) {
		if (n == myId)
			return pred;

		try {
			int p = mngr.getNodeAddress(n);
			Socket sock = new Socket(Manager.HOST, p);
			PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(
					sock.getInputStream()));

			Message msg = new Message(Message.GET_PREDECESSOR, n);
			out.println(msg.toString());

			String resp = in.readLine();
			sock.close();

			return Integer.parseInt(resp);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return n;
	}

	public void setPredecessor(int n, int id) {
		if (n == myId)
			pred = id;
		else {
			try {
				Socket sock = new Socket(Manager.HOST, mngr.getNodeAddress(n));
				PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(
						sock.getInputStream()));

				Message msg = new Message(Message.SET_PREDECESSOR, n, id);
				out.println(msg.toString());

				sock.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public int getId() {
		return this.myId;
	}
	
	public String sendKeys() {
		String keyString = "";
		for(int i : this.keys) {
			keyString += i + " ";
		}
		return keyString;
	}
	
	public int fingerLength() {
		int p = FINGER_LENGTH;
		return p;
	}
	
	/**
	 * i0 < id < i1
	 * 
	 * @param interval
	 * @param id
	 * @return
	 */
	private boolean inBetween(int[] interval, int id) {
		if (interval[0] > interval[1])
			return (interval[0] < id || id < interval[1]);
		return (interval[0] < id && id < interval[1]);

	}

	/**
	 * i0 <= id < i1
	 * 
	 * @param interval
	 * @param id
	 * @return
	 */
	private boolean inBetweenBP(int[] interval, int id) {
		// if(interval[0] == interval[1]) return true;
		if (interval[0] >= interval[1])
			return (interval[0] <= id || id < interval[1]);
		return (interval[0] <= id && id < interval[1]);

	}

	/**
	 * i0 < id <= i1
	 * 
	 * @param interval
	 * @param id
	 * @return
	 */
	private boolean inBetweenPB(int[] interval, int id) {
		// if(interval[0] == interval[1]) return true;
		if (interval[0] >= interval[1])
			return (interval[0] < id || id <= interval[1]);
		return (interval[0] < id && id <= interval[1]);

	}

	private boolean inBetweenBB(int[] interval, int id) {
		// if(interval[0] == interval[1]) return true;
		if (interval[0] > interval[1])
			return (interval[0] <= id || id <= interval[1]);
		return (interval[0] <= id && id <= interval[1]);

	}

	private void bind(int port) {
		try {
			serverSocket = new ServerSocket(port);

		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

	public void stop() {
		running.set(false);
		try {
			serverSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}