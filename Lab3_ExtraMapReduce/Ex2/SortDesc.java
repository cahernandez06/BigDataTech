import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.*;

@SuppressWarnings("rawtypes")
public class SortDesc implements WritableComparable {

	private int val;

	public int getValue() {
		return val;
	}

	public void setValue(int value) {
		this.val = value;
	}

	public void setValue(String value) {
		this.val = Integer.parseInt(value);
	}

	SortDesc() {

	}

	SortDesc(String s) {
		this.val = Integer.parseInt(s);
	}
	
	SortDesc(int s) {
		this.val = s;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		val = arg0.readInt();

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(val);

	}

	@Override
	public int hashCode() {
		return Objects.hash(val);
	}

	@Override
	public boolean equals(Object obj) {
		if (((SortDesc)obj).val == this.val) {
			return true;
		}
		return false;
	}

	@Override
	public String toString() {
		return String.valueOf(val);
	}

	@Override
	public int compareTo(Object o) {
        int thisValue = this.val;
        int thatValue = ((SortDesc)o).val;
        return (thisValue < thatValue ? 1 : (thisValue==thatValue ? 0 : -1));
      }
}
