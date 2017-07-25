package sortCompare;

import java.util.*;

/**
 * An interface for sorting algorithms
 */
interface Sorter<E extends Comparable<E>> {
	public void sort(ArrayList<E> data);
}
