package mp;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Manager {
	private static final int PORT = 7000;
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
		Node n0 = new Node(0, this, Manager.PORT);
		Thread t0 = new Thread(n0);
		nodes[0] = n0;
		threads[0] = t0;
		t0.start();
	}
	
	public int getNodeAddress(int id) {
		if(nodes[id] == null) return -1;
		return PORT + id;
	}
	
	public boolean containsNode(Node node, int p) {
		for(int i=0;i<Node.FINGER_LENGTH;i++) {
			if(node.fingers.get(i).getSuccessor() == p)
				return true;
		}
		
		return false;
	}
	
	private void output(String out) {
		
		if(logfile == null) {
			System.out.println(out);
		}else{ 
			FileWriter f;
			try {
				f = new FileWriter(logfile, true);
				f.write(out +"\n");
				f.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
			int p = 0;
			if(pInput.length != 1)
				p = Integer.parseInt(pInput[1]);
			if(p > 255) {
				p = (int)(p % Math.pow(2, nodes[0].fingerLength()));
			}
			if(input.startsWith("join ")) { //Join
				if(nodes[p] != null)
					continue;
				
				nodes[p] = new Node(p, 0, this, Manager.PORT + p);
				threads[p] = new Thread(nodes[p]);
				threads[p].start();
			} else if(input.startsWith("find ")) { //Find
				if(nodes[p] == null) //p is not an active node
					continue;
				
				int k = Integer.parseInt(pInput[2]);
				
				
			} else if(input.startsWith("leave ")) { //Leave
				if(nodes[p] == null) //p is not an active node
					continue;
				
				Node temp = nodes[p];
				
				nodes[temp.fingers.get(0).getSuccessor()].pred = temp.pred; //Update successor predecessor
				int count = 0;
				for(int i =nodes.length-1; i>=0;i--) {
					if(nodes[i] != null && containsNode(nodes[i], p)) {
						nodes[p].removeNode(i, p, temp.fingers.get(0).getSuccessor(), count);
						count++;
//						try {
//							System.out.println("remove from " + getNodeAddress(i));
//							Socket sock = new Socket(Manager.HOST, getNodeAddress(i));
//							PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
//
//							Message msg = new Message(Message.REMOVE_NODE, i, p, temp.fingers.get(0).getSuccessor());
//							out.println(msg.toString());
//
//							sock.close();
//
//						} catch (IOException e) {
//							e.printStackTrace();
//						}
					}
				}
				nodes[p] = null;
				temp.stop();
				try {
					threads[p].join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			} else if(input.startsWith("show ")) { //Show
				if(nodes[p] == null) //p is not an active node
					continue;
				
				Node n = nodes[p];
				for(Finger f: n.fingers) {
					System.out.println(f.getStart()+", " +f.getSuccessor());
				}
				
				output("Node: "+p+" Keys: " + n.sendKeys());
			} 
			else if(input.startsWith("show-all")) {
				for(Node n : nodes) {
					if(n != null) {
						output("Node: " + n.getId() + " Keys: " + n.sendKeys());
					}
				}
			}
				
			else {
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
