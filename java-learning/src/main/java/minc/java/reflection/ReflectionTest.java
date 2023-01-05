package minc.java.reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @Author: Minc
 * @DateTime: 2022.12.20 0020
 */
public class ReflectionTest {
    public static void main(String[] args) throws Exception {
        User user = new User("Minc", "123", 23);
        boolean skipJavaCheck = true;
        double sum = 0.0;
        for (int i = 0; i < 100; i++) {
            sum += testReflection(user, skipJavaCheck);
        }
        if (skipJavaCheck) {
            System.out.println("skip java check spent " + (sum / 100) + " ms");
        } else {
            System.out.println("don't skip java check spent " + (sum / 100) + " ms");
        }
//
//        Method getName = user.getClass().getDeclaredMethod("getName");
//        getName.setAccessible(true);
//        Object invoke = getName.invoke(user);
//        System.out.println(invoke);
//
//        Field name = user.getClass().getDeclaredField("name");
//
//        //取消java语言检查
//        name.setAccessible(true);
//        name.set(user, "Minc1");
//        System.out.println(user);
    }

    private static double testReflection(User user, Boolean skipJavaCheck) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        long start = System.nanoTime();
        //获得user类的静态方法out()
        Method out = user.getClass().getDeclaredMethod("out");
//        out.setAccessible(skipJavaCheck);
//        out.invoke(user);
        long end = System.nanoTime();
        return (end - start) * 1.0 / 1000 / 1000;
    }
}
