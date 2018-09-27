package handler;

import util.Catalog;

public class App {
public static Catalog model;
	
	public static void main(String[] args) {
        if (args != null && args.length == 2) {
            model = Catalog.getInstance(args[0],args[1]);
            Handler.init();
            Handler.parseSql();
           
        }
       
	}
}
