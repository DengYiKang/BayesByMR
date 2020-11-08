import java.lang.reflect.Method;

public class Test {
    public static void main(String[] args){
        try {
            Class<?> aClass = Class.forName(args[0]);
            Method method = aClass.getMethod("main", String[].class);
            String[] param=new String[args.length-1];
            System.arraycopy(args, 1, param, 0, param.length);
            method.invoke(null, (Object) param);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
