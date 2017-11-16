package edu.umn.cs.sthadoop.server;

import java.util.Random;

import org.apache.hadoop.util.PriorityQueue;


public class TopResults extends PriorityQueue<Point>{
	
	private Random r;
	
	/**
	 * Constructor to initialize the result with a size. 
	 * @param size
	 */
	public TopResults(int size) {
		super.initialize(size);
		r = new Random();
	}
	
	@Override
	public boolean insert(Point element) {
		// TODO Auto-generated method stub
		element.setPriority(r.nextInt());
		return super.insert(element);
	}
	
	

	@Override
	protected boolean lessThan(Object a, Object b) {
		// TODO Auto-generated method stub
		return ((Point) a).getPriority() >= ((Point) b).getPriority();
		
	}

}
