package sortCompare;

import java.util.ArrayList;

/**
 * An implementation of the Quicksort algorithm
 *
 * @param <E>
 *            the type of element to be sorted
 */
public class Quicksort<E extends Comparable<E>> implements Sorter<E> {

	/**
	 * Sorts the data using Quicksort
	 *
	 * @param data
	 *            Data to be sorted
	 */
	public void sort(ArrayList<E> data) {
		quicksortHelper(data, 0, data.size() - 1);
	}

	/**
	 * Helper method for Quicksort. Sorts data >= start and <= end
	 *
	 * @param data
	 *            data to be sorted
	 * @param start
	 *            start of the data to be sorted (inclusive)
	 * @param end
	 *            end of the data to be sorted (inclusive)
	 */
	private void quicksortHelper(ArrayList<E> data, int start, int end) {
		if (start < end) {
			int partition = partition(data, start, end);
			quicksortHelper(data, start, partition - 1);
			quicksortHelper(data, partition + 1, end);
		}
	}

	/**
	 * partitions the data based on the element at index end.
	 *
	 * @param data
	 *            data to be partitioned
	 * @param start
	 *            start of the data to be partitioned
	 * @param end
	 *            end of the data to be partitioned
	 * @return returned the index of the pivot element (after being copied into
	 *         the correct location)
	 */
	private int partition(ArrayList<E> data, int start, int end) {
		int lessThanIndex = start - 1;

		for (int i = start; i < end; i++) {
			if (data.get(i).compareTo(data.get(end)) < 1 || data.get(i).compareTo(data.get(end)) == 0) {
				lessThanIndex++;
				swap(data, lessThanIndex, i);
			}
		}

		swap(data, lessThanIndex + 1, end);

		return lessThanIndex + 1;
	}

	/**
	 * Swap two elements in the ArrayList
	 *
	 * @param data
	 *            data array
	 * @param index1
	 *            first element to be swapped
	 * @param index2
	 *            second element to be swapped
	 */
	private void swap(ArrayList<E> data, int index1, int index2) {
		E temp = data.get(index1);
		data.set(index1, data.get(index2));
		data.set(index2, temp);
	}
}
