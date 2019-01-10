package util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Chen Li, QinXuan Pian
 *
 */
public class UnionFind {
	private Map<String,String> parentMap = new HashMap<String,String>();
	private Map<String,Element> unionMap = new HashMap<String,Element>();
	private List<Element> elementList = new ArrayList<Element>();

	/**
	 * @param attrName
	 * @return element which included attrName
	 */
	public Element findElement(String attrName) {

		if(unionMap.get(attrName) == null) {
			parentMap.put(attrName, attrName);//root
			Element element = new Element();
			element.addAttr(attrName);
			this.unionMap.put(attrName, element);//key is root
			return element;
		}
		else {
			String root = findParent(attrName);//find root
			return this.unionMap.get(root);
		}
	}

	public String findParent(String childName) {
		if(parentMap.get(childName) == childName) {
			return childName;
		}
		else {
			return findParent(parentMap.get(childName));
		}
	}

	/**
	 * @param element1
	 * @param element2
	 */
	public void join(Element element1, Element element2) {
		String root1 = this.findParent(element1.getColumnAttrs().iterator().next());
		String root2 = this.findParent(element2.getColumnAttrs().iterator().next());
		if(!root1.equals(root2)) {
			parentMap.put(root1, root2);
			element2.addElement(element1);
			//		System.out.println("element1:"+element1);
			//		System.out.println("element2:"+element2);
			element2.setEquality(this.compareEquality(element1, element2));
			element2.setLowerB(this.compareLowerBound(element1, element2));
			element2.setUpperB(this.compareUpperBound(element1, element2));

		}
	}
	public Map<String,Element> getUnionMap(){
		return this.unionMap;
	}
	public String printUnionMap(){
		String str = "";
		for(String attr:this.unionMap.keySet()) {
			str = str + attr + ":";
			str += this.unionMap.get(attr).toString()+" ";
		}
		return str;
	}
	public List<Element> getElementList(){

		for(String attr: this.unionMap.keySet()) {
			String root = findParent(attr);
			Element element = this.unionMap.get(root);
			if(!this.elementList.contains(element)) {
				this.elementList.add(element);
			}
		}
		return this.elementList;
	}
	private Integer compareEquality(Element element1,Element element2) {
		if (element1.getEquality() == null | element2.getEquality() == null) {
			return element1.getEquality() == null ? element2.getEquality() : element1.getEquality();
		}else {
			return element1.getEquality();
		}
	}
	private Integer compareLowerBound(Element element1,Element element2) {
		if (element1.getLowerB() == null | element2.getLowerB() == null) {

			//		System.out.println("element1 low:"+element1.getLowerB());
			return element1.getLowerB() == null ? element2.getLowerB() : element1.getLowerB();
		}else {
			return element1.getLowerB() < element2.getLowerB() ? element2.getLowerB() : element1.getLowerB();
		}
	}

	private Integer compareUpperBound(Element element1,Element element2) {
		if (element1.getUpperB() == null | element2.getUpperB()  == null) {
			return element1.getUpperB()  == null ?element2.getUpperB()  : element1.getUpperB() ;
		}else {
			return element1.getUpperB()  < element2.getUpperB()  ? element1.getUpperB()  : element2.getUpperB() ;
		}
	}



}


