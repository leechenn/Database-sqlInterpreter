package util;

import java.util.HashSet;

public class Element {
private HashSet<String> columnAttrs = new HashSet<String>();
private Integer lowerB;
private Integer upperB;
private Integer equality;

public void addAttr(String columnAttr) {
	this.columnAttrs.add(columnAttr);
}

public HashSet<String> getColumnAttrs() {
	return this.columnAttrs;
}

public void setLowerB(Integer lowerB) {
	if(this.equality == null){
		this.lowerB = lowerB;
	}
//	this.lowerB = lowerB;
}

public Integer getLowerB() {
	return this.lowerB;
}

public void setUpperB(Integer upperB){
	if(this.equality == null) {
		this.upperB = upperB;
	}
//	 this.upperB = upperB;
}
public Integer getUpperB() {
	return this.upperB;
}
public void setEquality(Integer equality) {
//	System.out.println("equality:"+equality);
	this.equality = equality;
	if(equality!=null) {
	this.lowerB = equality;
	this.upperB = equality;
	}
}
public Integer getEquality() {
	return this.equality;
}
public void addElement(Element that) {
	this.columnAttrs.addAll(that.columnAttrs);
}
@Override
public String toString() {
	return "UnionFindElement [ufe=" + columnAttrs + ", lowerBound=" + this.lowerB + ", upperBound=" + this.upperB
			+ ", equalityConstraint=" + this.equality + "]";
}
public String elementInfo() {
	return "[" + columnAttrs + ", equals " + this.equality + ", min " + this.lowerB
			+ ", max " + this.upperB + "]";
}
}




