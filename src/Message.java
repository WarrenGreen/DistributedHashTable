import java.util.ArrayList;
import java.util.List;


public class Message {
	public static final int FIND_SUCCESSOR = 0;
	public static final int GET_SUCCESSOR = 1;
	public static final int CLOSEST_PRECEDING = 2;
	
	private int type;
	private int id;
	private int from;
	
	private List<Integer> path;
	
	public Message() {
		path = new ArrayList<Integer>();
	}
	
	public Message(String input) {
		parseInput(input);
	}
	
	public Message(int type, int id) {
		this.type = type;
		this.id = id;
	}
	
	private void parseInput(String input) {
		String[] pInput = input.split(" ");
		
		this.type = Integer.parseInt(pInput[0]);
		this.id = Integer.parseInt(pInput[1]);
		this.from = Integer.parseInt(pInput[1]);
	}
	
	public String toString() {
		return type + " " + id + " " + from;
	}

	public int getId() {
		return id;
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

	public int getFrom() {
		return from;
	}

	public void setFrom(int from) {
		this.from = from;
	}

}
