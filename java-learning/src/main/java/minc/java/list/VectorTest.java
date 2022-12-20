package minc.java.list;

import java.util.Vector;

/**
 * @Author: Minc
 * @DateTime: 2022.12.19 0019
 */
public class VectorTest {
    public static void main(String[] args) {
        Vector<String> vector = new Vector<>(6);
        for (int i = 0; i < 10000; i++) {
            vector.add(String.valueOf(i));
            //capacity扩容扩大之前的2倍
            System.out.println("now capacity is " + vector.capacity());
        }
    }
}
