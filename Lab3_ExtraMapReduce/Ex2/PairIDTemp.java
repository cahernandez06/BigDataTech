import java.io.*;
import java.util.Objects;

import org.apache.hadoop.io.*;

public class PairIDTemp implements WritableComparable<PairIDTemp> {

  private String first;
  private SortDesc second;
  
  public PairIDTemp() {
  }
  
  public PairIDTemp(String first, SortDesc second) {
    set(first, second);
  }
  
  public void set(String first, SortDesc second) {
    this.first = first;
    this.second = second;
  }
  
  public String getFirst() {
    return first;
  }

  public SortDesc getSecond() {
    return second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(first);
    out.writeInt(second.getValue());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first = in.readUTF();
    second = new SortDesc(in.readInt());
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(first, second);
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof PairIDTemp) {
    	PairIDTemp ip = (PairIDTemp) o;
      return first == ip.first && second == ip.second;
    }
    return false;
  }

  @Override
  public String toString() {
    return first + "\t" + second;
  }
  
  @Override
  public int compareTo(PairIDTemp ip) {
    int cmp = first.compareTo(ip.getFirst());
    if (cmp != 0) {
      return cmp;
    }
    return second.compareTo(ip.getSecond());
  }  
}