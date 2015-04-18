package mp;
import java.util.ArrayList;
import java.util.List;


public class Message {
	public static final int FIND_SUCCESSOR = 0;
	public static final int GET_SUCCESSOR = 1;
	public static final int GET_PREDECESSOR = 2;
	public static final int SET_PREDECESSOR = 3;
	public static final int CLOSEST_PRECEDING = 4;
	public static final int UPDATE_FINGERS = 6;
	public static final int UPDATE_SUCCESSOR = 7;
	public static final int REMOVE_NODE = 8;
	public static final int MOVE_KEY = 9;
	public static final int MOVE_KEY_DELETE = 10;
	
	private int type;
	private int id;
	private int n;
	private int nPrime = -1;
	//private String[] keyString;
	private List<Integer> keyString;
	
	private List<Integer> path;
	
	public Message() {
		path = new ArrayList<Integer>();
	}
	
	public Message(String input) {
		parseInput(input);
	}
	
	public Message(int type, int n) {
		this.type = type;
		this.n = n;
	}
	
	public Message(int type, int n, int id) {
		this.type = type;
		this.n = n;
		this.id = id;
	}
	
	public Message(int type, int n, int id, int nPrime) {
		this.type = type;
		this.n = n;
		this.id = id;
		this.nPrime = nPrime;
	}
	
	public Message(int type, int n, int id, int filler, List<Integer> keyString) {
		this.type = type;
		this.n = n;
		this.id = id;
		this.nPrime = filler;
		this.keyString = keyString;
	}
	
	private void parseInput(String input) {
		String[] pInput = input.split(" ");
		
		this.type = Integer.parseInt(pInput[0]);
		this.n = Integer.parseInt(pInput[1]);
		this.id = Integer.parseInt(pInput[2]);
		
		if(pInput.length > 3) {
			this.nPrime = Integer.parseInt(pInput[3]);
		}
		
		if(pInput.length > 4) {
			//this.keyString = pInput[4];
		}
		System.out.println("I AM HERE");
	}
	
	public String toString() {
		String out = type + " " + n + " " + id;
		
		if(nPrime != -1)
			out += " " + nPrime;
		
		if(keyString != null)
			out += " " + keyString;
		System.out.println("wut " + out);
		System.out.println("Keystring: " + keyString);
		return out;
	}

	public int getId() {
		return id;
	}
	
	public List<Integer> getKey() {
		return keyString;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getN() {
		return n;
	}

	public void setN(int n) {
		this.n = n;
	}

	public int getNPrime() {
		return nPrime;
	}

	public void setNPrime(int nPrime) {
		this.nPrime = nPrime;
	}

}