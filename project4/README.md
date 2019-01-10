# Database-sqlInterpreter

Project4

Chen Li, Qinxuan Pian

App.java and Handler.java are the top-level classes of the program. App takes user input and calls Handler, which initiates the file I/O and parsing. 


1. The selction pushing

The Union-Find visitor is used for expression after where in query, this visitor in util package will help to build a union-find data structure for attributes in the expression and classify the expression into to parts, one is for union-find elements and the other one is residual expression for join conditions.  After building union-find data structure, we can get more comprehensive selection condition for each table and build a multiple join logical tree.


2. The choice of implementation for each logical selection opreator
    This functionality is implemented in the class src/util/SelectOptimizer.java. When the physical visitor a SelectOperator, it will determine whether the child Scan Operator should use index or not according to the cost of specific selection method, if an index can be used, the selection condition will be splited into two parts, one is for index scan and the other one will be tranmitted into selection operator.

3. The choice of join order
    This functionality is implemented in the class src/util/JoinOrderDeterminator.java. Given a list of tables that need joining, the first step is to get all the subsets of these tables. It can be implemented using DFS (function getAllSubsets()). The second step is to build a cost map using buttom-up dynamic programming. The key of the map is a subset of the tables. And the value of the map is planCostInfo.

4. The choice of each join operator
    This functionality is implemented in the PhysicalPlanBuilder, there is a method called isAllEqual in this class.  The SMJ operator is faster that the BNLJ operator. However, the SMJ can only be used for the equality join, the isAllEqual method is used for verifying whether the join is a equality join.  We use SMJ for all the equality join conditions. As for the other cases, we use BNLJ operator. We use sizea 5 for BNLJ, and 4 pages for SMJ. 