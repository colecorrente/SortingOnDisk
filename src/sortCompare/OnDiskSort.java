package sortCompare;

import java.io.*;
import java.util.*;

/**
 * Sorts the data on-disk, by sorting the data in small chunks and then merging
 * the data into one larger chunk
 *
 * @author Nicholas Corrente @ 2/11/2017
 */
public class OnDiskSort {

	// the sorter to be used
	private Sorter<String> sorter;

	// maxSize of each temp file
	private int maxSize;

	// directory to save files in
	private File workingDirectory;

	// the list of temperary files
	private ArrayList<File> fileList;

	/**
	 * Creates a new sorter for sorting string data on disk. The sorter operates
	 * by reading in maxSize worth of data elements (in this case, Strings) and
	 * then sorts them using the provided sorter. It does this chunk by chunk
	 * for all of the data, at each stage writing the sorted data to temporary
	 * files in workingDirectory. Finally, the sorted files are merged together
	 * (in pairs) until there is a single sorted file. The final output of this
	 * sorting should be in outputFile
	 *
	 * @param maxSize
	 *            the maximum number of items to put in a chunk
	 * @param workingDirectory
	 *            the directory where any temporary files created during sorting
	 *            should be placed
	 * @param sorter
	 *            the sorter to use to sort the chunks in memory
	 */
	public OnDiskSort(int maxSize, File workingDirectory, Sorter<String> sorter) {
		this.workingDirectory = workingDirectory;
		this.maxSize = maxSize;
		this.sorter = sorter;

		this.fileList = new ArrayList<File>();

		// create directory if it doesn't exist
		if (!workingDirectory.exists()) {
			workingDirectory.mkdir();
		}
	}

	/**
	 * Remove all files that that end with fileEnding in workingDirectory
	 *
	 * If you name all of your temporary files with the same file ending, for
	 * example ".temp_sorted" then it's easy to clean them up using this method
	 *
	 * @param workingDirectory
	 *            the directory to clear
	 * @param fileEnding
	 *            clear only those files with fileEnding
	 */
	private void clearOutDirectory(File workingDirectory, String fileEnding) {
		for (File file : workingDirectory.listFiles()) {
			if (file.getName().endsWith(fileEnding)) {
				file.delete();
			}
		}
	}

	/**
	 * Write the data in dataToWrite to outfile one String per line
	 *
	 * @param outfile
	 *            the output file
	 * @param dataToWrite
	 *            the data to write out
	 */
	private void writeToDisk(File outfile, ArrayList<String> dataToWrite) {
		try {
			PrintWriter out = new PrintWriter(new FileOutputStream(outfile));

			for (String s : dataToWrite) {
				out.println(s);
			}

			out.close();
		} catch (IOException e) {
			throw new RuntimeException(e.toString());
		}
	}

	/**
	 * Copy data from fromFile to toFile
	 *
	 * @param fromFile
	 *            the file to be copied from
	 * @param toFile
	 *            the destination file to be copied to
	 */
	private void copyFile(File fromFile, File toFile) {
		try {
			BufferedReader in = new BufferedReader(new FileReader(fromFile));
			PrintWriter out = new PrintWriter(new FileOutputStream(toFile));

			String line = in.readLine();

			while (line != null) {
				out.println(line);
				line = in.readLine();
			}

			out.close();
			in.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Sort the data in data using an on-disk version of sorting
	 *
	 * @param dataReader
	 *            an Iterator for the data to be sorted
	 * @param outputFile
	 *            the destination for the final sorted data
	 */
	public void sort(Iterator<String> dataReader, File outputFile) {
		ArrayList<String> tempSort = new ArrayList<String>(this.maxSize);

		int fileNumber = 0;
		// loop through the entire data file
		while (dataReader.hasNext()) {
			tempSort.clear();

			// split the data in to files of maxSize length
			for (int i = 0; i < this.maxSize; i++) {
				if (dataReader.hasNext()) {
					tempSort.add(dataReader.next());
				}
			}

			// sort the small chunk
			this.sorter.sort(tempSort);

			// write the sorted chunk to a temp file
			File tempFile = new File(workingDirectory + "/" + fileNumber + ".tempfile");
			writeToDisk(tempFile, tempSort);
			fileList.add(tempFile);

			fileNumber++;
		}

		// merge and clean up
		mergeFiles(this.fileList, outputFile);
		clearOutDirectory(workingDirectory, "tempfile");
	}

	/**
	 * Merges all the Files in sortedFiles into one sorted file, whose
	 * destination is outputFile.
	 *
	 * @pre All of the files in sortedFiles contained data that is sorted
	 * @param sortedFiles
	 *            a list of files containing sorted data
	 * @param outputFile
	 *            the destination file for the final sorted data
	 * @post the array list of sorted files will be in one large sorted file:
	 *       outputFile.
	 */
	protected void mergeFiles(ArrayList<File> sortedFiles, File outputFile) {
		try {
			File mergedTemp = new File(workingDirectory + "/mergedTemp.tempfile");
			mergedTemp.createNewFile();

			for (int i = 0; i < sortedFiles.size(); i++) {
				merge(mergedTemp, sortedFiles.get(i), mergedTemp);
			}

			copyFile(mergedTemp, outputFile);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Given two files containing sorted strings, one string per line, merge
	 * them into one sorted file
	 *
	 * @param file1
	 *            file containing sorted strings, one per line
	 * @param file2
	 *            file containing sorted strings, one per line
	 * @param outFile
	 *            destination file for the results of merging the two files
	 * @post the two files are merged into one file and written to: outFile
	 */
	protected void merge(File file1, File file2, File outFile) {
		File tempOut = new File(workingDirectory + "/singleMerge.tempfile");

		try {
			BufferedReader file1Reader = new BufferedReader(new FileReader(file1));
			BufferedReader file2Reader = new BufferedReader(new FileReader(file2));

			PrintWriter outWriter = new PrintWriter(new FileOutputStream(tempOut));

			String file1S = file1Reader.readLine();
			String file2S = file2Reader.readLine();

			// compare the first string from each file and append that string to
			// the temp file
			while (file1S != null && file2S != null) {
				if (file1S.compareTo(file2S) < 1) {
					outWriter.println(file1S);
					file1S = file1Reader.readLine();
				} else {
					outWriter.println(file2S);
					file2S = file2Reader.readLine();
				}
			}

			// copy over remaining data from file1 if there is any
			while (file1S != null) {
				outWriter.println(file1S);
				file1S = file1Reader.readLine();
			}

			// copy over remaining data from file2 if there is any
			while (file2S != null) {
				outWriter.println(file2S);
				file2S = file2Reader.readLine();
			}

			file1Reader.close();
			file2Reader.close();
			outWriter.close();

			copyFile(tempOut, outFile);

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Create a sorter that does a mergesort in memory Create a diskSorter to do
	 * external merges Use subdirectory "sorting_run" of your project as the
	 * working directory Create a word scanner to read King's "I have a dream"
	 * speech. Sort all the words of the speech and put them in dile data.sorted
	 *
	 * @param args
	 *            -- not used!
	 */
	public static void main(String[] args) {
		MergeSort<String> sorter = new MergeSort<String>();
		OnDiskSort diskSorter = new OnDiskSort(10, new File("sorting_run"), sorter);

		WordScanner scanner = new WordScanner(new File("sorting_run//Ihaveadream.txt"));

		System.out.println("running");
		diskSorter.sort(scanner, new File("sorting_run//data.sorted"));
		System.out.println("done");
	}

}
