package edu.umn.cs.sthadoop.server;

/**
 * Created by saifalharthi on 6/12/14.
 */
public class Point {
    private Long id;
    private double lat;
    private double lon;

    public Point() {
    }

    public Point(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public Point(String lat, String lon) {
        this.lat = Double.parseDouble(lat);
        this.lon = Double.parseDouble(lon);
    }

    public Point(String id, String lat, String lon) {
        this.id = Long.parseLong(id);
        this.lat = Double.parseDouble(lat);
        this.lon = Double.parseDouble(lon);
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {
        return lon;
    }

    @Override
    public String toString() {
        return this.id + "," + this.lat + "," + this.lon;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
    /**
     * if the passed parameter is greater than the invoker.
     * @param obj
     * @return
     */
    public boolean isGreater(Point obj){
        if(this.lat <= obj.getLat() && this.lon <= this.getLon() )
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

            if (s1.getId().equals(this.getId())) {
                return true;
            }
        }
        return false;
    }
}

