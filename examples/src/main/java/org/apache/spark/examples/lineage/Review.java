package org.apache.spark.examples.lineage;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class Review implements Writable, Serializable {

    public int rater_id;
    public byte rating;

    public Review() {
        rater_id = -1;
        rating = 0;
    }

    public Review(Review a){
        rater_id = a.rater_id;
        rating = a.rating;
    }
    public void clear(){
        rater_id = -1;
        rating = 0;
    }
    public void readFields(DataInput in) throws IOException {
        rater_id = in.readInt();
        rating = in.readByte();
    }

    public void write (DataOutput out) throws IOException {
        out.writeInt(rater_id);
        out.writeByte(rating);
    }
}
