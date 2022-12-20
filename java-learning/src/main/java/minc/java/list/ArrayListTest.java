package minc.java.list;

import java.util.ArrayList;

/**
 * @Author: Minc
 * @DateTime: 2022.12.19 0019
 */
public class ArrayListTest {
    public static void main(String[] args) {
        ArrayList<String> strings = new ArrayList<>(10);
        for (int i = 0; i < 10000; i++) {
            strings.add(String.valueOf(i));
            //size扩容到之前size的1.5倍
            System.out.println("now size is " + strings.size());
            System.out.println();
        }
    }
}
