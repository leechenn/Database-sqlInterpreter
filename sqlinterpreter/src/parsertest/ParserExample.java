package parsertest;
import java.io.FileReader;
import java.util.Arrays;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

/**
 * Example class for getting started with JSQLParser. Reads SQL statements from
 * a file and prints them to screen; then extracts SelectBody from each query
 * and also prints it to screen.
 * 
 * @author Lucja Kot
 */
public class ParserExample {

	private static final String queriesFile = "samples/input/queries.sql";

	public static void main(String[] args) {
		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
			Statement statement;
			while ((statement = parser.Statement()) != null) {
				System.out.println("Read statement: " + statement);
				Select select = (Select) statement;
				PlainSelect ps = (PlainSelect)select.getSelectBody();
				String item = ps.getFromItem().toString();
				String[] strs = item.split("\\s+");
				System.out.println("projection---"+ps.getSelectItems());
				System.out.println("tableArray: "+Arrays.toString(strs));
				System.out.println("fromItem: "+ps.getFromItem());
				System.out.println("whereItem: "+ps.getWhere());
			
				
				try {
				System.out.println("join---"+ps.getJoins());}
				catch(NullPointerException e) {
					e.getMessage();
					System.out.println();
				}
				System.out.println("Select body is " + select.getSelectBody());
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
}