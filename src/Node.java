package src;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class Node implements Runnable {

	private int myId;
	public List<Finger> fingers;
	private int pred; // predecessor
	private List<Integer> keys;
	private AtomicBoolean running;
	private Manager mngr;

	private static final int FINGER_LENGTH = 3;

	private ServerSocket serverSocket;

	/**
	 * Used for first node.
	 * 
	 * @param id
	 * @param keys
	 */
	public Node(int id, Manager mngr) {
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

		pred = -1;

		this.mngr = mngr;
		// queue = new LinkedBlockingQueue<Message>();
		running = new AtomicBoolean();
		running.set(true);
		bind(mngr.getNodeAddress(myId));
	}

	public Node(int id, int nPrime, Manager mngr) {
		this.myId = id;

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

		bind(mngr.getNodeAddress(myId));
		
		initFingers(nPrime);
		updateOthers(myId);
		
	}

	@Override
	public void run() {
		while (running.get()) {

			String input = null;
			try {
				Socket clientSocket = serverSocket.accept();
				PrintWriter out = new PrintWriter(
						clientSocket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(
						clientSocket.getInputStream()));
				if ((input = in.readLine()).compareTo(Manager.STOP) == 0)
					break;

				Message msg = new Message(input);

				if (msg.getType() == Message.FIND_SUCCESSOR) {
					out.println(findSuccessor(myId, msg.getId()));
				} else if(msg.getType() == Message.GET_SUCCESSOR) {
					out.println(getSuccessor(msg.getN()));
				}else if(msg.getType() == Message.GET_PREDECESSOR) {
					out.println(getPredecessor(msg.getN()));
				} else if(msg.getType() == Message.SET_PREDECESSOR) {
					setPredecessor(msg.getN(), msg.getId());
				} else if(msg.getType() == Message.CLOSEST_PRECEDING) {
					out.println(closestPrecedingFinger(msg.getN(), msg.getId()));
				} else if(msg.getType() == Message.UPDATE_FINGERS) {
					updateFingers(msg.getN(), msg.getId(), msg.getIndex());
				} else if(msg.getType() == Message.UPDATE_OTHERS) {
					updateOthers(msg.getN());
				}

				clientSocket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public void initFingers(int nPrime) {
		fingers.get(0).setSuccessor(findSuccessor(nPrime, myId));
		pred = getPredecessor(fingers.get(0).getSuccessor());
		this.setPredecessor(myId, findPredecessor(getSuccessor(myId), getSuccessor(myId)));
		
		for(int i=0;i<Node.FINGER_LENGTH-1; i++) {
			if(inBetweenBP(new int[]{myId, fingers.get(i).getSuccessor()}, fingers.get(i+1).getStart())) {
				fingers.get(i+1).setSuccessor(fingers.get(i).getSuccessor());
			} else {
				fingers.get(i+1).setSuccessor(findSuccessor(nPrime, fingers.get(i+1).getStart()));
			}
		}
	}

	public int findSuccessor(int n, int id) {
		if( n == myId) {
			if(inBetweenPB(new int[]{n, getSuccessor(n)}, id))
				return getSuccessor(n);
			
			int nPrime = findPredecessor(n, id);
			return getSuccessor(nPrime);
		} else {
			try {
				Socket sock = new Socket(Manager.HOST, mngr.getNodeAddress(n));
				PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(
						sock.getInputStream()));

				Message msg = new Message(Message.FIND_SUCCESSOR, id);
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
		while(!inBetweenPB(new int[]{nPrime, getSuccessor(nPrime)}, id)) {
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
				if (inBetweenBP(new int[] {n, id}, s))
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
		if(n == myId) {
			for(int i=0;i<Node.FINGER_LENGTH; i++) {
				int pPrime = Math.abs((int) (n - Math.pow(2, i) % Math.pow(2, Node.FINGER_LENGTH)));
				int p = findPredecessor(n, pPrime);
				updateFingers(p, n, i);
			}
		} else {
			try {
				Socket sock = new Socket(Manager.HOST, mngr.getNodeAddress(n));
				PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(
						sock.getInputStream()));

				Message msg = new Message(Message.UPDATE_OTHERS, n);
				out.println(msg.toString());

				sock.close();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public void updateFingers(int n, int s, int i) {
		if( n == myId) {
			if(inBetweenBP(new int[]{n, fingers.get(i).getSuccessor()}, s)) {
				fingers.get(i).setSuccessor(s);
				int p = findPredecessor(myId, myId);
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
		if(n == myId)
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
		if(n == myId)
			return pred;
		
		try {
			Socket sock = new Socket(Manager.HOST, mngr.getNodeAddress(n));
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
		if(n == myId)
			pred = id;
		
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

	/**
	 * i0 < id < i1
	 * @param interval
	 * @param id
	 * @return
	 */
	private boolean inBetween(int[] interval, int id) {
		
		return (interval[0] < id && id < interval[1]);
		
	}
	
	/**
	 * i0 <= id < i1
	 * @param interval
	 * @param id
	 * @return
	 */
	private boolean inBetweenBP(int[] interval, int id) {
		return (interval[0] <= id && id < interval[1]);

	}
	
	/**
	 * i0 < id <= i1
	 * @param interval
	 * @param id
	 * @return
	 */
	private boolean inBetweenPB(int[] interval, int id) {
		return (interval[0] < id && id <= interval[1]);

	}
	
	private boolean inBetweenBB(int[] interval, int id) {
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
		;
	}
}