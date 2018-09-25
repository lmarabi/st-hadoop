package edu.umn.cs.sthadoop.trajectory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import edu.umn.cs.sthadoop.core.STPoint;


/**
 * This class implements the Dynamic Time Warping algorithm given two sequences of trajectory
 * 
 * <pre>
 *   X = x1, x2,..., xi,..., xn
 *   Y = y1, y2,..., yj,..., ym
 * </pre>
 * 
 * @author louai Alarabi
 */

public class DTW {

	List<STPoint> traj1;
	List<STPoint> traj2;
	protected STPoint[] seq1;
	protected STPoint[] seq2;
	protected int[][] warpingPath;

	protected int n;
	protected int m;
	protected int K;

	protected double warpingDistance;

	/**
	 * Constructor
	 *
	 * @param firstTrajectory
	 * @param SecondTrajectory
	 */
	public DTW( List<STPoint> traj1, List<STPoint> traj2) {
		seq1 = new STPoint[traj1.size()];
		seq2 = new STPoint[traj2.size()];
		traj1.toArray(seq1);
		traj2.toArray(seq2);
		n = seq1.length;
		m = seq2.length;
		K = 1;

		warpingPath = new int[n + m][2]; // max(n, m) <= K < n + m
		warpingDistance = 0.0;

		this.compute();
	}
	

	public void compute() {
		double accumulatedDistance = 0.0;

		double[][] d = new double[n][m]; // local distances
		double[][] D = new double[n][m]; // global distances

		for (int i = 0; i < n; i++) {
			for (int j = 0; j < m; j++) {
				d[i][j] = seq1[i].distanceTo(seq2[j].x, seq2[j].y);
			}
		}

		D[0][0] = d[0][0];

		for (int i = 1; i < n; i++) {
			D[i][0] = d[i][0] + D[i - 1][0];
		}

		for (int j = 1; j < m; j++) {
			D[0][j] = d[0][j] + D[0][j - 1];
		}

		for (int i = 1; i < n; i++) {
			for (int j = 1; j < m; j++) {
				accumulatedDistance = Math.min(
						Math.min(D[i - 1][j], D[i - 1][j - 1]), D[i][j - 1]);
				accumulatedDistance += d[i][j];
				D[i][j] = accumulatedDistance;
			}
		}
		accumulatedDistance = D[n - 1][m - 1];

		int i = n - 1;
		int j = m - 1;
		int minIndex = 1;

		warpingPath[K - 1][0] = i;
		warpingPath[K - 1][1] = j;

		while ((i + j) != 0) {
			if (i == 0) {
				j -= 1;
			} else if (j == 0) {
				i -= 1;
			} else { // i != 0 && j != 0
				double[] array = { D[i - 1][j], D[i][j - 1], D[i - 1][j - 1] };
				minIndex = this.getIndexOfMinimum(array);

				if (minIndex == 0) {
					i -= 1;
				} else if (minIndex == 1) {
					j -= 1;
				} else if (minIndex == 2) {
					i -= 1;
					j -= 1;
				}
			} // end else
			K++;
			warpingPath[K - 1][0] = i;
			warpingPath[K - 1][1] = j;
		} // end while
		warpingDistance = accumulatedDistance / K;

		this.reversePath(warpingPath);
	}

	/**
	 * Changes the order of the warping path (increasing order)
	 *
	 * @param path
	 *            the warping path in reverse order
	 */
	protected void reversePath(int[][] path) {
		int[][] newPath = new int[K][2];
		for (int i = 0; i < K; i++) {
			for (int j = 0; j < 2; j++) {
				newPath[i][j] = path[K - i - 1][j];
			}
		}
		warpingPath = newPath;
	}

	/**
	 * Returns the warping distance
	 *
	 * @return
	 */
	public double getDistance() {
		return warpingDistance;
	}


	/**
	 * Finds the index of the minimum element from the given array
	 *
	 * @param array
	 *            the array containing numeric values
	 * @return the min value among elements
	 */
	protected int getIndexOfMinimum(double[] array) {
		int index = 0;
		double val = array[0];

		for (int i = 1; i < array.length; i++) {
			if (array[i] < val) {
				val = array[i];
				index = i;
			}
		}
		return index;
	}

	/**
	 * Returns a string that displays the warping distance and path
	 */
	public String toString() {
		String retVal = "Warping Distance: " + warpingDistance + "\n";
		retVal += "Warping Path: {";
		for (int i = 0; i < K; i++) {
			retVal += "(" + warpingPath[i][0] + ", " + warpingPath[i][1] + ")";
			retVal += (i == K - 1) ? "}" : ", ";

		}
		return retVal;
	}

	/**
	 * Tests this class
	 *
	 * @param args ignored
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ParseException {
		List<STPoint> query = new ArrayList<STPoint>();
		List<STPoint> trajectory = new ArrayList<STPoint>();
		// query trajectory 
		query.add(new STPoint( "2008-05-14 01:43:26,39.9119983,116.606835"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9119783,116.6065483"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9119599,116.6062649"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9119416,116.6059899"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9119233,116.6057282"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9118999,116.6054783"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9118849,116.6052366"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9118666,116.6050099"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.91185,116.604775"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9118299,116.604525"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9118049,116.6042649"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.91177,116.6040166"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9117516,116.6037583"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9117349,116.6035066"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9117199,116.6032666"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9117083,116.6030232"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9117,116.6027566"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.91128,116.5969383"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9112583,116.5966766"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9112383,116.5964232"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9112149,116.5961699"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9111933,116.5959249"));
		query.add(new STPoint( "2008-05-14 01:43:26,39.9111716,116.5956883"));
		// trajectory 
		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9119983,116.606835"));
		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9119783,116.6065483"));
		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9119599,116.6062649"));
		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9119416,116.6059899"));
		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9119233,116.6057282"));
		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9118999,116.6054783"));
		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9118849,116.6052366"));
		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9118666,116.6050099"));
		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.91185,116.604775"));
//		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9118299,116.604525"));
//		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9118049,116.6042649"));
//		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.91177,116.6040166"));
//		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9117516,116.6037583"));
//		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9117349,116.6035066"));
//		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9117199,116.6032666"));
//		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9117083,116.6030232"));
//		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9117,116.6027566"));
//		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.91128,116.5969383"));
//		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9112583,116.5966766"));
//		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9112383,116.5964232"));
//		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9112149,116.5961699"));
//		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9111933,116.5959249"));
//		trajectory.add(new STPoint( "2008-05-14 01:43:26,39.9111716,116.5956883"));
		
		DTW dtw = new DTW(trajectory, query);
		System.out.println(dtw.getDistance());
		System.out.println(dtw);
	}
}