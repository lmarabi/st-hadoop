package edu.umn.cs.sthadoop.server;

/**
 * Created by Louai Alarabi
 */
public class MBR {

    private Point max;
    private Point min;

    public MBR() {
    }
    

    public MBR(Point min, Point max) {
        this.min = min;
        this.max = max;
    }

    public Point getMain() {
        return min;
    }

    public Point getMax() {
        return max;
    }

    public Point getMin() {
        return min;
    }

    public void setMin(Point min) {
        this.min = min;
    }

    public void setMax(Point max) {
        this.max = max;
    }

    /**
     * This method check whether the MBR is intersect with the MB
     *
     * @param pmax
     * @param pmin
     * @return true if there is intersect otherwise it will return false
     */
    public boolean Intersect(Point pmax, Point pmin) {
        //RectA1: this.main = x1,y1 ; this.max = x2,y2
        //RectB2: pmin = x3,y3 ; pmax= x4,y4
        //return !(x2 < x3 || x1 > x2 || y2 > y3 || y1 < y4)
        //if (RectA.X1 < RectB.X2 && RectA.X2 > RectB.X1 &&
        //    RectA.Y1 < RectB.Y2 && RectA.Y2 > RectB.Y1) 
        if (this.min.getY() <= pmax.getY()
                && this.max.getY() >= pmin.getY()
                && this.min.getX() <= pmax.getX()
                && this.max.getX() >= pmin.getX()) {
            return true;
        } else {
            return false;
        }
    }
    
    public boolean Intersect(MBR mbr) {
    	//RectA1: this.main = x1,y1 ; this.max = x2,y2
        //RectB2: pmin = x3,y3 ; pmax= x4,y4
        //return !(x2 < x3 || x1 > x2 || y2 > y3 || y1 < y4)
        //if (RectA.X1 < RectB.X2 && RectA.X2 > RectB.X1 &&
        //    RectA.Y1 < RectB.Y2 && RectA.Y2 > RectB.Y1) 
        if (this.min.getY() <= mbr.getMax().getY()
                && this.max.getY() >= mbr.getMin().getY()
                && this.min.getX() <= mbr.getMax().getX()
                && this.max.getX() >= mbr.getMin().getX()) {
            return true;
        } else {
            return false;
        }
	}

    /**
     * This method check whether the point inside the MBR or not
     *
     * @param point
     * @return true if the point inside the area other wise return false
     */
    public boolean insideMBR(Point point) {
        if (point.getX() <= this.max.getX()
                && point.getX() >= this.min.getX()
                && point.getY() <= this.max.getY()
                && point.getY() >= this.min.getY()) {
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * This method return the width of the MBR 
     * @return
     */
    public double getWidth(){
    	return this.max.getY() - this.min.getY();
    }
    
    
    /**
     * This method return the Height of the MBR 
     * @return
     */
    public double getHeight(){
    	return this.max.getX() - this.min.getX();
    }
    
    /**
     * This method convert the MBR object into WKT format as string object. 
     * @return
     */
    public String toWKT(){
    	return "POLYGON (("
    		    +this.max.getY() + " "+ this.min.getX() 
    		    +", "+ this.max.getY() + " "+ this.max.getX()
    		    +", "+ this.min.getY() + " "+ this.max.getX()
    		    +", "+ this.min.getY() + " "+ this.min.getX()
    		    +", "+ this.max.getY() + " "+ this.min.getX()
    		    + "))";
    }
    
    @Override
    public String toString() {
    	// TODO Auto-generated method stub
    	return "maxlon "+ this.max.getY()+ " maxlat " + this.max.getX()+
    			" minlon "+ this.min.getY()+ " minlat " + this.min.getX();
    }

	

}
