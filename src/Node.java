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
	private List<Finger> fingers;
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

		pred = 0;

		this.mngr = mngr;
		// queue = new LinkedBlockingQueue<Message>();
		running = new AtomicBoolean();
		running.set(true);
		bind(mngr.getNodeAddress(myId));
	}

	public Node(int id, int nPrime, Manager mngr) {
		this.myId = id;

		fingers = new ArrayList<Finger>();
		pred = -1;

		this.mngr = mngr;
		running = new AtomicBoolean();
		running.set(true);

		bind(mngr.getNodeAddress(myId));
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
				}

				clientSocket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private void initFingers() {

	}

	public int findSuccessor(int n, int id) {
		if( n == myId) {
			int nPrime = findPredecessor(n, id);
			return getSuccessor(nPrime);
		} else {
			try {
				Socket sock = new Socket(Manager.HOST, n);
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
				if (inBetween(new int[] {n + 1, id}, fingers.get(i)
						.getSuccessor()))
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

	/**
	 * i0 < id < i1
	 * @param interval
	 * @param id
	 * @return
	 */
	private boolean inBetween(int[] interval, int id) {
		int start, end;
		if (interval[0] < interval[1]) {
			start = interval[0];
			end = interval[1];
		} else {
			start = interval[1];
			end = interval[0];
		}

		return (start < id && id < end);

	}
	
	/**
	 * i0 <= id < i1
	 * @param interval
	 * @param id
	 * @return
	 */
	private boolean inBetweenBP(int[] interval, int id) {
		int start, end;
		if (interval[0] < interval[1]) {
			start = interval[0];
			end = interval[1];
		} else {
			start = interval[1];
			end = interval[0];
		}
		
		if(start == end && id == start)
			return true;

			return (start <= id && id < end);

	}
	
	/**
	 * i0 < id <= i1
	 * @param interval
	 * @param id
	 * @return
	 */
	private boolean inBetweenPB(int[] interval, int id) {
		int start, end;
		if (interval[0] < interval[1]) {
			start = interval[0];
			end = interval[1];
		} else {
			start = interval[1];
			end = interval[0];
		}

		if(start == end && id == start)
			return true;
		
		return (start < id && id <= end);

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
