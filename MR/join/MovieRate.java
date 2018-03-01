package MR.join;

import com.sun.tools.javah.Gen;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Time;

public class MovieRate implements WritableComparable<MovieRate>{
    private String UserID;
    private String Gender;
    private int Age;
    private String Occupation;
    private String Zipcode;
    private String MovieID;
    private double Rating;
    private String Timestamped;
    public MovieRate(){}

    public MovieRate(String userID, String gender, int age, String occupation, String zipcode, String movieID, double rating, String timestamped) {
        super();
        UserID = userID;
        Gender = gender;
        Age = age;
        Occupation = occupation;
        Zipcode = zipcode;
        MovieID = movieID;
        Rating = rating;
        Timestamped = timestamped;
    }

    public String getUserID() {
        return UserID;
    }

    public void setUserID(String userID) {
        UserID = userID;
    }

    public String getGender() {
        return Gender;
    }

    public void setGender(String gender) {
        Gender = gender;
    }

    public int getAge() {
        return Age;
    }

    public void setAge(int age) {
        Age = age;
    }

    public String getOccupation() {
        return Occupation;
    }

    public void setOccupation(String occupation) {
        Occupation = occupation;
    }

    public String getZipcode() {
        return Zipcode;
    }

    public void setZipcode(String zipcode) {
        Zipcode = zipcode;
    }

    public String getMovieID() {
        return MovieID;
    }

    public void setMovieID(String movieID) {
        MovieID = movieID;
    }

    public double getRating() {
        return Rating;
    }

    public void setRating(double rating) {
        Rating = rating;
    }

    public String getTimestamped() {
        return Timestamped;
    }

    public void setTimestamped(String timestamped) {
        Timestamped = timestamped;
    }

    @Override
    public int compareTo(MovieRate mr) {
        int i = mr.getUserID().compareTo(this.getUserID());
        if(i==0){
            return mr.getMovieID().compareTo(this.getMovieID());
        }else
            return i;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(UserID);
        out.writeUTF(Gender);
        out.writeInt(Age);
        out.writeUTF(Occupation);
        out.writeUTF(Zipcode);
        out.writeUTF(MovieID);
        out.writeDouble(Rating);
        out.writeUTF(Timestamped);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.UserID = in.readUTF();
        this.Gender = in.readUTF();
        this.Age = in.readInt();
        this.Occupation = in.readUTF();
        this.Zipcode = in.readUTF();
        this.MovieID = in.readUTF();
        this.Rating = in.readDouble();
        this.Timestamped = in.readUTF();

    }
}
