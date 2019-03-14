import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class pairTC implements WritableComparable<pairTC> {

  private double temp;
  private int count;
  
  public pairTC() {
  }
  
  public pairTC(double temp, int count) {
    this.temp = temp;
    this.count = count;
  }
  
  public double getTemp() {
    return temp;
  }

  public int getCount() {
    return count;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof pairTC) {
    	pairTC p = (pairTC) o;
      return temp == p.temp && count == p.count;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return (int) (temp * 163 + count);
  }
  
  @Override
  public int compareTo(pairTC p) {
    int comp = compare(temp, p.temp);
    if (comp != 0) 
      return comp;
    return compare(count, p.count);
  }
  
  public static int compare(double a, double b) {
    return (a < b ? -1 : (a == b ? 0 : 1));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
	  // TODO Auto-generated method stub
	  temp = in.readDouble();
	  count = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
	  // TODO Auto-generated method stub
	  out.writeDouble(temp);
	  out.writeInt(count);
  }

}
