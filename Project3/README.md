# Database-sqlInterpreter

Project3

Chen Li, Qinxuan Pian

App.java and Handler.java are the top-level classes of the program. App takes user input and calls Handler, which initiates the file I/O and parsing. 

The lowkey and highkey should be retrieved from the selection condition if the index key can be used for that query, we have a method in the Class Tool called retrieveIdxAttr, in this method, at the beginning, it tells whether the index could be used in selection, if it could, the selection condition will be divided into two portions, one can be handled by index key and the other one cannot, lowkey and highkey
pair is translated by the first portion.

The different handling for clustered and unclustered is processed in the getnextTuple method in indexScanOperator.  If the index tree is clustered, the start <pageId, tupleId> pair is retrieved and then read data file successively.

The root-to-leaf tree descent is processed in Class TreeDeserializer, we only deserialize leafNode which contains dataEntry for selection after the start leafNode is found using binary search method and address of index Node from top to bottom.

For logical tree building, we have logicalSelectOperator for every base table.  In physicalPlanBuilder, when a logicalSelectOperator is visited, we do a verification to see if index should be used for selection, if index should be used, we call the method in Tool Class to check whether the index in index file could be used in our sql, if it could, we build an index-scan Operator and a full-scan Operator to process the selection.


The Experiments.pdf is in benchMarking folder