package util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import logicaloperators.LogicalMultiJoinOperator;

public class JoinOrder {
	
	// dynamic programming to decide the order of join
	
	// input: relations
	// output: relations in optimal order
	
	public LogicalMultiJoinOperator lmjo;
	public List<String> relations;
	public HashMap<ArrayList<String>, ArrayList<Object>> opt;
	// Optimal table
	// relations joined, (estimated join size, ordering)
	private ArrayList<ArrayList<String>> subsets;
	private int minCost;
	private ArrayList<String> optOrder;
	
	public JoinOrder(LogicalMultiJoinOperator l) {
		this.lmjo = l;
	}
	
	public ArrayList<String> getOrderedRelations() {
		relations = lmjo.tableList;
		// singleton table first
		ArrayList<String> result = new ArrayList<String>();
		switch (relations.size()){
			case 1:
				result.add(relations.get(0));
				return result;

			case 2:
				if (numTuple(relations.get(0))>numTuple(relations.get(1))) {
					result.add(relations.get(1));
					result.add(relations.get(0));
				}
				else {
					result.add(relations.get(0));
					result.add(relations.get(1));
				}
				return result;
			default: // 3 or more relations => use DP to solve
				subsets = findSubsets(relations, 2);
				for (ArrayList<String> subset : subsets) {
					ArrayList<String> key = new ArrayList<String>();
					ArrayList<Object> value = new ArrayList<Object>();
					ArrayList<String> pairOrder = new ArrayList<String>();
					int size = 0;
					// TODO: compute the joined size!
					if (numTuple(subset.get(0)) > numTuple(subset.get(1))) {
						pairOrder.add(subset.get(1));
						pairOrder.add(subset.get(0));
					}
					else {
						pairOrder.add(subset.get(0));
						pairOrder.add(subset.get(1));
					}
					key = subset;
					Collections.sort(key);
					value.add(size);
					value.add(pairOrder);
					opt.put(key, value);
				}				
				for (int k = 3; k <= relations.size(); k++) {
					subsets = findSubsets(relations, k);
					for (ArrayList<String> subset : subsets) {
						minCost = Integer.MAX_VALUE;
						optOrder = new ArrayList<String>();
						for (String r : subset) {
							ArrayList<String> rest = subset;
							rest.remove(r);
							Collections.sort(rest);
							// find the cost in the OPT table => size
							int cost = (int) opt.get(rest).get(0);
							if (cost < minCost) {
								minCost = cost;
								optOrder = (ArrayList<String>) opt.get(rest).get(1);
								optOrder.add(r);
							}
						}
						// TODO: compute size of joined result => function
						ArrayList<Object> content = new ArrayList<Object>();
						content.add(1);
						content.add(optOrder);
						Collections.sort(subset);
						opt.put(subset, content);
					}
					
				}
				
			}
		Collections.sort(relations);
		return (ArrayList<String>) opt.get(relations).get(1);
	}
	
	
	// helper functions
	
	public int numTuple(String relation) {
		// TODO: find the number of tuples in this relation
		return 0;
	}
	
	public static ArrayList<ArrayList<String>> findSubsets(List<String> relations2, int size) {
		
		int n = relations2.size();
		ArrayList<ArrayList<String>> result = new ArrayList<ArrayList<String>>();
		
		// Run a loop from 0 to 2^n
		for (int i = 0; i < (1<<n); i++) {
			ArrayList<String> temp = new ArrayList<String>();
			int m = 1; // m is used to check set bit in binary representation.
			for (int j = 0; j < n; j++) {
				if ((i & m) > 0){
					temp.add(relations2.get(j));
				}
				m = m << 1;
			}
			if (temp.size() == size) {
				result.add(temp);
			}
		}
		return result;
	}
	
	public static void main (String[] args) {
		ArrayList<String> x = new ArrayList<String>();
		x.add("a");
		x.add("b");
		x.add("c");
		x.add("d");
		System.out.println(findSubsets(x, 2));
	}
}
