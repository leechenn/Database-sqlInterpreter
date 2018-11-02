# Database-sqlInterpreter
App.java and Handler.java are the top-level classes of the program. App takes user input and calls Handler, which initiates the file I/O and parsing.

For join condition extraction, we basically followed the suggested strategy.  We used an embedded list to represent the order of conjuncts being executed, which has the exact same order as a left deep tree. The order follows the rule that the join conditions that involves any table that's closer to the end of the list must be executed later.  The ExpressionVisitor that we implemented -- JoinExtract is to extract every conjunction in expression after where clause and assign them into two types of lists, one for signle table and another for join condition.  In JoinExtract class, the comments are writen for the details in how the methods are used for assigning different kinds of expression.  For join condition list, the expression in each list node corresponds to the join condition from left to right in the tree and for single table expression list, the list node also corresponds to tables from left to right. 

Note that the output directory will not be cleared at the beginning of the program. For instance, if a valid query2 is executed and the program outputs a file, then if the program is rerun and query2 is now invalid, the output file will still be the one from the valid query2. 

All the file separators appear in the Catalog class. 
