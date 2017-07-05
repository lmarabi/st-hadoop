package edu.umn.cs.sthadoop.server;

/**
 * Created by saifalharthi on 6/12/14.
 */
public class MBR {

    private Point max;
    private Point min;

    public MBR() {
    }
    
    public MBR(String parsString){
    	String[] token = parsString.split(" ");
    	this.max = new Point(token[3], token[1]);
    	this.min = new Point(token[7], token[5]);
    }
    
    /**
     * This method take MBR.toString() and convert it to MBR object
     * @param stringValue
     * @return
     */
    public void parse(String stringValue){
    	MBR temp = new MBR();
    	String[] token = stringValue.split(" ");
    	this.max = new Point(token[3], token[1]);
    	this.min = new Point(token[7], token[5]);
    	
    }

    public MBR(Point max, Point min) {
        this.max = max;
        this.min = min;
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
        if (this.min.getLon() <= pmax.getLon()
                && this.max.getLon() >= pmin.getLon()
                && this.min.getLat() <= pmax.getLat()
                && this.max.getLat() >= pmin.getLat()) {
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
        if (this.min.getLon() <= mbr.getMax().getLon()
                && this.max.getLon() >= mbr.getMin().getLon()
                && this.min.getLat() <= mbr.getMax().getLat()
                && this.max.getLat() >= mbr.getMin().getLat()) {
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
        if (point.getLat() <= this.max.getLat()
                && point.getLat() >= this.min.getLat()
                && point.getLon() <= this.max.getLon()
                && point.getLon() >= this.min.getLon()) {
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
    	return this.max.getLon() - this.min.getLon();
    }
    
    
    /**
     * This method return the Height of the MBR 
     * @return
     */
    public double getHeight(){
    	return this.max.getLat() - this.min.getLat();
    }
    
    /**
     * This method convert the MBR object into WKT format as string object. 
     * @return
     */
    public String toWKT(){
    	return "POLYGON (("
    		    +this.max.getLon() + " "+ this.min.getLat() 
    		    +", "+ this.max.getLon() + " "+ this.max.getLat()
    		    +", "+ this.min.getLon() + " "+ this.max.getLat()
    		    +", "+ this.min.getLon() + " "+ this.min.getLat()
    		    +", "+ this.max.getLon() + " "+ this.min.getLat()
    		    + "))";
    }
    
    @Override
    public String toString() {
    	// TODO Auto-generated method stub
    	return "maxlon "+ this.max.getLon()+ " maxlat " + this.max.getLat()+
    			" minlon "+ this.min.getLon()+ " minlat " + this.min.getLat();
    }

	

}
