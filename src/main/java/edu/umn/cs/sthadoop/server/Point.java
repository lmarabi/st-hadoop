package edu.umn.cs.sthadoop.server;

/**
 * Created by Louai Alarabi
 */
public class Point {
	
    private double x;
    private double y;

    public Point() {
    }

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public Point(String x, String y) {
        this.x = Double.parseDouble(x);
        this.y = Double.parseDouble(y);
    }

    public void setX(double x) {
        this.x = x;
    }

    public void setY(double y) {
        this.y = y;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    @Override
    public String toString() {
        return  this.x + "," + this.y;
    }

    /**
     * if the passed parameter is greater than the invoker.
     * @param obj
     * @return
     */
    public boolean isGreater(Point obj){
        if(this.x <= obj.getX() && this.y <= this.getY() )
            return true;
        else
            return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj instanceof Point) {
            Point s1 = (Point) obj;

            if (s1.x == this.x && s1.y == this.y) {
                return true;
            }
        }
        return false;
    }
}

