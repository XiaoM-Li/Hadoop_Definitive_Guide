package hadoop.market.basket.analysis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class MBATool {
	public static void main(String[] args) {
			
		Collection<String> elements=new ArrayList<>();
		elements.add("a");
		elements.add("b");
		elements.add("c");
		elements.add("d");
		List<List<String>> combinations = findSortedCombinations(elements, 3);
		System.out.println(combinations);
	}
	
	public static List<String> StringToSortedList(String line){
		
		List<String> list=new ArrayList<>();
		String value=line.trim();
		String[] values=value.split(",");
		for(String item:values){
			list.add(item);
		}
		
		Collections.sort(list);
		return list;
		
	}
	
	public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements,int n){
		
		List<List<T>> result=new ArrayList<List<T>>();
		if(n==0){
			result.add(new ArrayList<T>());
			return result;
		}
		
		List<List<T>> combinations=findSortedCombinations(elements, n-1);
		
		for(List<T> combination:combinations){
			for(T element:elements){
				if(combination.contains(element)){
					continue;
				}
				
				List<T> list=new ArrayList<>();
				list.addAll(combination);
				
				if(list.contains(element)){
					continue;
				}
				
				list.add(element);
				Collections.sort(list);
				
				if(result.contains(list)){
					continue;
				}
				result.add(list);
			}
		}
		return result;
	}
	
}
