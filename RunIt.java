package code;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;

public class RunIt {
    public static void main(String[] args) {
        String fileLocation = "C:\\Users\\John\\Documents\\00_UNI\\2014-15\\project\\logparser\\logs\\";
        File f = new File(fileLocation);

        ArrayList<File> logs = new ArrayList<File>(Arrays.asList(f.listFiles()));

            logs.stream().forEach(

                    item -> {
                        try {
                            LogFactory.makeLog(fileLocation + item.getName());
                        } catch (ClassNotFoundException e) {

                        } catch (IllegalAccessException e) {

                        } catch (InstantiationException e) {

                        } catch (NoSuchMethodException e) {

                        } catch (InvocationTargetException e) {

                        }
                    }
            );
    }
}
