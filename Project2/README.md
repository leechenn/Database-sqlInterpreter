# Database-sqlInterpreter

Project2

Chen Li, Qinxuan Pian

App.java and Handler.java are the top-level classes of the program. App takes user input and calls Handler, which initiates the file I/O and parsing. 

Our implementation of SMJ does not keep unbound state because the merging is done in a lazy manner. It maintains a few pointers, one pointing to the current inner tuple, one pointing to the start of the inner partition. When SMJ gets a tuple from its outer child, it compares that to the inner partition and moves it or the inner partition pointer until they match. A pointer will run on the inner partition, and each consecutive call to getNextTuple() will output a joined tuple of the current outer tuple and the current inner tuple. The inner tuple pointer then moves one down. When the inner tuple no longer matches the current inner partition, get a new outer tuple from the outer child and repeat the above algorithm. Only up to three tuples are saved in memory at the same time at most: current inner partition, current outer tuple, current inner tuple. Therefore SMJ does not keep unbound state. 

Our distinct were implemented using sort and thus does not keep unbounded state. 

The way we were planning to implement reset(index) is consistent with how the writeup suggests. 

The PhysicalPlanBuilder is integrated into handler.java. Logical operators and physical operators are placed under the corresponding package/folder with the same names. 

For benchmarking, we compare the performance of BNLJ, SMJ and TNLJ with three queries, the data we choose is about 5000 tuples for each table and the test data is in testData folder.  Test queries are listed below.  The queries we used to test contain equijoins only and the join tree contain at least one equality condition in every join node, the reason for which is to display that SMJ will perform best for equijoins queries compared with the other two algorithms.  The buffer size for BNLJ is setted as 1, 5 and 10 for comparing the impact of buffer size on BNLJ algorithm.  The detail results and a benchmarking histogram is shown in benmarking.pdf.

Query1: SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;
Query2: SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D;
Query3: SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D AND Sailors.B < 150;