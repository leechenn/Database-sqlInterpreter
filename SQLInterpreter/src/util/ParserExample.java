package util;
import java.io.FileReader;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
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

	private static final String queriesFile = "queries.sql";

	public static void main(String[] args) {
		try {
			CCJSqlParser parser = new CCJSqlParser(new FileReader(queriesFile));
			Statement statement;
			while ((statement = parser.Statement()) != null) {
				System.out.println("Read statement: " + statement);
				Select select = (Select) statement;
				PlainSelect ps = (PlainSelect) select.getSelectBody();
				System.out.println(ps.getFromItem());
				System.out.println(ps.getFromItem().getAlias());
				System.out.println(ps.getJoins());
				if (ps.getJoins()!=null && ps.getJoins().size()>1) {
					System.out.println( ((Join) ps.getJoins().get(2)).getRightItem().getAlias());
				}
				System.out.println("Select body is " + select.getSelectBody());
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
}