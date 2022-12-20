package minc.java.dynamincproxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.text.SimpleDateFormat;

/**
 * 动态代理，用代理类控制原始接口或者类的行为
 * @Author: Minc
 * @DateTime: 2022.12.14 0014
 */
public class DynamicProxy {
    public static void main(String[] args) {
        People coder = new Coder("Minc");
        InvocationHandler    coderInvoker = new CoderInvoker(coder);
        People people = (People) Proxy.newProxyInstance(coder.getClass().getClassLoader(), coder.getClass().getInterfaces(), coderInvoker);
        System.out.println(people.eat());
        System.out.println(people.sleep());
    }
}

interface People {
    State sleep();

    State eat();
}

class Coder implements People {
    protected String name;

    public Coder(String name) {
        this.name = name;
    }

    @Override
    public State sleep() {
        System.out.println("people who name " + name + " and work as coder will sleeping");
        return State.FINISH;
    }

    @Override
    public State eat() {
        System.out.println("people who name " + name + " and work as coder will eating");
        return State.FINISH;
    }
}

enum State {
    FINISH,
    READY,
    WILL
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
