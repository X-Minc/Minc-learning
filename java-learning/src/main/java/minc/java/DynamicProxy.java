package minc.java;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.text.SimpleDateFormat;

/**
 * @Author: Minc
 * @DateTime: 2022.12.14 0014
 */
public class DynamicProxy {
    public static void main(String[] args) {
        People coder = new Coder("Minc");
        InvocationHandler coderInvoker = new CoderInvoker(coder);
        People people = (People) Proxy.newProxyInstance(coder.getClass().getClassLoader(), coder.getClass().getInterfaces(), coderInvoker);
        people.eat();
        people.sleep();
    }
}

interface People {
    void sleep();

    void eat();
}

class Coder implements People {
    protected String name;

    public Coder(String name) {
        this.name = name;
    }

    @Override
    public void sleep() {
        System.out.println("people who name " + name + " and work as coder will sleeping");
    }

    @Override
    public void eat() {
        System.out.println("people who name " + name + " and work as coder will eating");
    }
}

class CoderInvoker implements InvocationHandler {
    protected Object object;
    private final SimpleDateFormat format;

    public CoderInvoker(Object object) {
        this.object = object;
        format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        System.out.println("now " + format.format(System.currentTimeMillis()));
        return method.invoke(object, args);
    }
}
