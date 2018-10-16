package handler;

import util.Catalog;

/**
 * @author Chen Li, Qinxuan Pian
 * Upper Class for input output path
 */
public class App {
	
public static Catalog model;
	
	public static void main(String[] args) {
        if (args != null && args.length == 2) {
        	System.out.println(args[0]);
        	System.out.println(args[1]);
            model = Catalog.getInstance(args[0],args[1]);//create a singleton for Catalog and store it as static variable in App named model
            Handler.init();
            Handler.parseSql();
        }
       
	}
}
