package minc.java.list;

import java.util.LinkedList;

/**
 * @Author: Minc
 * @DateTime: 2022.12.19 0019
 */
public class LinkListTest {
    public static void main(String[] args) {
        LinkedList<String> strings = new LinkedList<>();
        for (int i = 0; i < 10000; i++) {
            strings.add(String.valueOf(i));
            //size大小弹性扩容
            //具备链表容易增加和删除节点的功能
            System.out.println("now size is " + strings.size());
            System.out.println();
        }
    }
}
