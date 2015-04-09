import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Manager {
	private static final int PORT = 8000;
	public static final String HOST = "127.0.0.1";
	public static final String STOP = "STOP";
	
	private String logfile = null;
	private Thread[] threads;
	private Node[] nodes;
	
	public Manager(String logfile) {
		this.logfile = logfile;
		
		threads = new Thread[256];
		nodes = new Node[256];
	}
	
	private void addFirst() {
		List<Integer> keys = new ArrayList<Integer>();
		for(int i=0;i<256;i++)
			keys.add(i);
		Node n0 = new Node(0, this);
		Thread t0 = new Thread(n0);
		nodes[0] = n0;
		threads[0] = t0;
		t0.start();
	}
	
	public int getNodeAddress(int id) {
		return PORT + id;
	}
	
	public void start() {
		addFirst();
		
		Scanner in = new Scanner(System.in);
		String input;
		while((input = in.nextLine()).compareTo("exit") != 0) {
			if(input.compareTo("show all") == 0) { //Show all
				
				continue;
			}
			
			String[] pInput = input.split(" ");
			int p = Integer.parseInt(pInput[1]);
			if(input.startsWith("join ")) { //Join
				if(nodes[p] != null)
					continue;
				
				nodes[p] = new Node(p, p-1, this);
				threads[p] = new Thread(nodes[p]);
				threads[p].start();
			} else if(input.startsWith("find ")) { //Find
				if(nodes[p] == null) //p is not an active node
					continue;
				
				int k = Integer.parseInt(pInput[2]);
				
				
			} else if(input.startsWith("leave ")) { //Leave
				if(nodes[p] == null) //p is not an active node
					continue;
			} else if(input.startsWith("show ")) { //Show
				if(nodes[p] == null) //p is not an active node
					continue;
				
				Node n = nodes[p];
				for(Finger f: n.fingers)
					System.out.println(f.getStart()+", " +f.getSuccessor());
			} else {
				System.out.println("Invalid Command.");
			}
		}
		
		stop();
		
	}
	
	private void stop() {
		for(int i=0;i<nodes.length;i++){
			Node n = nodes[i];
			if(n != null) {
				n.stop();
				try {
					Socket sock = new Socket(Manager.HOST, getNodeAddress(i));
					new PrintWriter(sock.getOutputStream(), true).println(Manager.STOP);
					sock.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		for(Thread t: threads){
			try {
				if(t != null)
					t.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}
	
	public static void main(String[] args) {
		String logfile = null;
		for(int i=0;i<args.length;i++) {
			if(args[i].compareTo("-h") == 0)
				printHelp();
			if(args[i].compareTo("-g") == 0)
				logfile = args[i+1];
		}
		
		Manager mngr = new Manager(logfile);
		mngr.start();
	}
	
	private static void printHelp() {
		System.out.println("usage: -g logfile");
		System.exit(1);
	}
}
