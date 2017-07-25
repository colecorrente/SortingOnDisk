package sortCompare;

import java.util.ArrayList;

/**
 * Implementation of the MergeSort algorithm
 *
 * @param <E>
 *            type of data to be sorted
 */
public class MergeSort<E extends Comparable<E>> implements Sorter<E> {

	/**
	 * Sort the ArrayList of data using MergeSort
	 *
	 * @param list
	 *            data to be sorted
	 */
	public void sort(ArrayList<E> data) {
		sortHelper(data, 0, data.size());
	}

	/**
	 * MergeSort helper method. Sorts data >= start and < end
	 *
	 * @param list
	 *            data to be sorted
	 * @param low
	 *            start of the data to be sorted
	 * @param high
	 *            end of the data to be sorted (exclusive)
	 */
	private void sortHelper(ArrayList<E> data, int low, int high) {
		if (high - low > 1) {
			int mid = low + (high - low) / 2;

			sortHelper(data, low, mid);
			sortHelper(data, mid, high);
			merge(data, low, mid, high);
		}
	}

	/**
	 * Merge data >= low and < high into sorted data. Data >= low and < mid are
	 * in sorted order. Data >= mid and < high are also in sorted order
	 *
	 * @param data
	 *            the partially sorted data
	 * @param low
	 *            bottom index of the data to be merged
	 * @param mid
	 *            midpoint of the data to be merged
	 * @param high
	 *            end of the data to be merged (exclusive)
	 */
	public void merge(ArrayList<E> data, int low, int mid, int high) {
		ArrayList<E> temp = new ArrayList<E>(high - low);

		int lowIndex = low;
		int midIndex = mid;

		while (lowIndex < mid && midIndex < high) {
			if (data.get(lowIndex).compareTo(data.get(midIndex)) < 1) {
				temp.add(data.get(lowIndex));
				lowIndex++;
			} else {
				temp.add(data.get(midIndex));
				midIndex++;
			}
		}

		// copy over the remaining data on the low to mid side if there
		// is some remaining.
		while (lowIndex < mid) {
			temp.add(data.get(lowIndex));
			lowIndex++;
		}

		// copy over the remaining data on the mid to high side if there
		// is some remaining. Only one of these two while loops should
		// actually execute
		while (midIndex < high) {
			temp.add(data.get(midIndex));
			midIndex++;
		}

		// copy the data back from temp to list
		for (int i = 0; i < temp.size(); i++) {
			data.set(i + low, temp.get(i));
		}
	}
}
