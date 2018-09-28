package entity;

import java.util.Arrays;

public class Tuple {
    private int[] data; // string array to store data
    private String dataString;

   
    public Tuple(String s) {
    	this.dataString = s;
        String[] sData = s.split(",");
        data = new int[sData.length];
        for(int i = 0; i < sData.length; i++){
            data[i] = Integer.parseInt(sData[i]);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Tuple tuple = (Tuple) o;

        return Arrays.equals(data, tuple.data);
    }
    public int[] getData() {
    	return this.data;
    }


    @Override
    public String toString() {
       return dataString;
    }
    public static void main(String[] args) {
		Tuple tuple = new Tuple("3,4,5");
		System.out.println(tuple);
	}
}
