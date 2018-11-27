package util;

import java.security.KeyStore.Entry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import handler.App;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;

public class SelectOptimizer {
	static HashMap<String, Integer[]> attInfo; 
	static List<String> attName;
	static List<Double> cost;
	static double plainScanCost;
	public static IndexInfo whichIndexToUse(String tableName, Expression exp) {
		
		attInfo = new HashMap<String, Integer[]>();
		attName = new ArrayList<String>();
		cost = new ArrayList<Double>();
		plainScanCost = -1;
		
		List<Expression> exps = Tool.decompAnds(exp);
		System.out.println("expression:" + exps);
		
		for(Expression ex : exps){
			
			Expression left =((BinaryExpression)ex).getLeftExpression();
			Expression right =((BinaryExpression)ex).getRightExpression();
			String attr = "";

			if(left instanceof Column && right instanceof LongValue) {
				attr = ((Column)left).toString().split("\\.")[1];				
			} else if(right instanceof Column && left instanceof LongValue){
				attr = ((Column)right).toString().split("\\.")[1];
			} else if(right instanceof Column && left instanceof Column){
				//calculate plain cost		
				plainScanCost = (App.model.getStats().getStatsMap().get(tableName).getTupleNum() *
						App.model.getStats().getStatsMap().get(tableName).getAttrsName().length * 4)/4096;
				continue;
			}else if (ex instanceof EqualsTo){
				plainScanCost = (App.model.getStats().getStatsMap().get(tableName).getTupleNum() *
						App.model.getStats().getStatsMap().get(tableName).getAttrsName().length * 4)/4096;
				continue;
			}
			String[] in = {attr};
			Integer[] range = Tool.getSelRange(ex,in);
			updateAttInfo(attr,range);			
		}
	
		double pageNum = (App.model.getStats().getStatsMap().get(tableName).getTupleNum() *
				App.model.getStats().getStatsMap().get(tableName).getAttrsName().length * 4)/4096;
		// calculate each attr's cost
		Set<Map.Entry<String,Integer[]>> set = attInfo.entrySet();
		// each entry is a attr name : range
		for(Map.Entry<String,Integer[]> entry : set){
			IndexInfo indexInfo = App.model.getIndexInfo(tableName, entry.getKey());	
			double localCost = -1;
			if(indexInfo == null){ // only plain scan			
				localCost = pageNum;
			} else { // has index 
				int[] minMax = App.model.getStats().getStatsMap().get(tableName).getInfoMap().get(entry.getKey());
				double maxRange = minMax[1] - minMax[0];
				double curLow = 0;
				double curHigh = 0;	
				//check max
				if(entry.getValue()[0] == null){
					curHigh = minMax[1];
				}
				//check min
				if(entry.getValue()[1] == null){
					curLow = minMax[0];
				}
				// calculate reduction factor
				double rf =(curHigh - curLow) /maxRange;
				
				if(indexInfo.clust){ // if cur attr has clustered index
					localCost = 3 + pageNum * rf;
				} else {
					double leafNum = indexInfo.getNumOfLeafNodes();
					localCost = 3 + (leafNum + App.model.getStats().getStatsMap().get(tableName).getTupleNum())*rf;
				}
			}
			attName.add(entry.getKey());
			cost.add(localCost);	
		}
		// check which attr has the lowerest cost
		if(plainScanCost !=-1) {
			if(cost.size()!=0){
				for(double c : cost){
					if(plainScanCost < c){
						return null;
					}
				}
			}
		}
		if(cost.size()==0){
			return null;
		}
		if(attName.size() != cost.size()){
			throw new IllegalArgumentException();
		}
		int minIndex = 0;
		double minCost = cost.get(0);
		for(int i = 1; i < cost.size(); i++){
			if(minCost > cost.get(i)){
				minCost = cost.get(i);
				minIndex = i;
			}
		}
		return App.model.getIndexInfo(tableName, attName.get(minIndex));	
		
	}
	/** 
	 * update the range for each of my attr
	 * 
	 */
	public static void updateAttInfo(String attr, Integer[] range){
		if(!attInfo.containsKey(attr)){
			Integer[] r = new Integer[2];
			for(int i = 0; i < range.length; i++){
				r[i] = range[i];
			}
			attInfo.put(attr, r);
		} else {
			Integer[] preRange = attInfo.get(attr);		
		// update min  
		if(range[0]!=null){
			if(preRange[0] == null){
				preRange[0] = range[0];
			} else {
				preRange[0] = Math.max(preRange[0],range[0]);
				attInfo.put(attr, preRange);
			}
		}
		//update max
		if(range[1]!=null){
			if(preRange[1] == null){
				preRange[1] = range[1];
			} else {
				preRange[1] = Math.min(preRange[1],range[1]);				
				attInfo.put(attr, preRange);
			}
		}			
		}
	}
}
