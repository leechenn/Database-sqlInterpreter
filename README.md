# Database-sqlInterpreter
App.java and Handler.java are the top-level classes of the program. App takes user input and calls Handler, which initiates the file I/O and parsing.

For join condition extraction, we basically followed the suggested strategy. We did not explicitly implement the tree, but we used an embedded list to represent the order of conjuncts being executed, which has the exact same order as a left deep tree. The order follows the rule that the join conditions that involves any table that's closer to the end of the list must be executed later. We implemented another ExpressionVisitor -- JoinExtract to replace the conjunct with an Expression and put them in order in a list. Then we recursively join the two leftmost Expressions in the list until there's nothing left to join and get the result.

Note that the output directory will not be cleared at the beginning of the program. For instance, if a valid query2 is executed and the program outputs a file, then if the program is rerun and query2 is now invalid, the output file will still be the one from the valid query2. 

All the file separators appear in the Catalog class. 
