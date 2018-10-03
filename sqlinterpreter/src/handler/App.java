package handler;

import util.Catalog;

/**
 * @author Chen Li, QinXuan Pian
 * Upper Class for input output path
 */
public class App {
	
public static Catalog model;
	
	public static void main(String[] args) {
        if (args != null && args.length == 2) {
            model = Catalog.getInstance(args[0],args[1]);//create a singleton for Catalog and store it as static variable in App named model
            Handler.init();
            Handler.parseSql();
        }
       
	}
}
