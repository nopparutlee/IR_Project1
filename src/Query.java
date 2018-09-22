

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Query {

	// Term id -> position in index file
	private  Map<Integer, Long> posDict = new TreeMap<Integer, Long>();
	// Term id -> document frequency
	private  Map<Integer, Integer> freqDict = new TreeMap<Integer, Integer>();
	// Doc id -> doc name dictionary
	private  Map<Integer, String> docDict = new TreeMap<Integer, String>();
	// Term -> term id dictionary
	private  Map<String, Integer> termDict = new TreeMap<String, Integer>();
	// Index
	private  BaseIndex index = null;
	

	//indicate whether the query service is running or not
	private boolean running = false;
	private RandomAccessFile indexFile = null;
	
	/* 
	 * Read a posting list with a given termID from the file 
	 * You should seek to the file position of this specific
	 * posting list and read it back.
	 * */
	private  PostingList readPosting(FileChannel fc, int termId)
			throws IOException {
		/*
		 * TODO: Your code here
		 * DONE by Earth 
		 * 
		 */

		//Step 1: Find the corresponding postingList is posting.dict that has the same termID as termId
		
			//Find the term index position from the corresponding termID in posDict
			Long positionInCorpus;	//Term position in the corpus.index
			Set<Integer> posDictKeys = posDict.keySet();
			for(Integer termIDkey : posDictKeys) {
				
				if(termID == termIDkey) { positionInCorpus = posDict.get(termIDkey); }
				
			}
			
			//Find the document frequency from the corresponding termID in freqDict
			int docFrequency = 0;						//Number of documents that the term appears in
			
			Set<Integer> freqDictKeys = freqDict.keySet();
			for(Integer freqIDkey : freqDictKeys) {
				
				if(termID == freqIDkey) { docFrequency = freqDict.get(freqIDkey); }
			}
		
		
		//Step 2: From the correspodning positingList, use termPos to set FileChannel's position in corpus.index
		fc.position(positionInCorpus);
		
		
		//Step 3: From corpus.index, read the <termID, DocFreq, {DocIDs}> tuple into the ByteBuffer (size = 4+4+(4*DocFreq)}
		int numberOfBytes = 4+4+(4*docFrequency);				//Size of byte array to keep the byte read f
		byte arr[] = new byte[numberOfBytes];
		
		ArrayList<Integer> decimal = new ArrayList<Integer>();	//ArrayList to keep the converted byte numbers
		
		fc.read(arr); 											//Read the byte numbers into the array.
		ByteBuffer byteBuffer = ByteBuffer.wrap(arr);			//Wrap the array into byteBuffer
		
		while(byteBuffer.hasRemaining()) {
			
			decimal.add(byteBuffer.getInt());					//Store the values from byteBuffer in decimal format (4 byte / 1 decimal)
		}

		//Step 4: Instantiate new PostingList using termID and {DocIDs}
		int i;
		
		List<Integer> docIDs = new ArrayList<Integer>();
		
		for(i=1;i<decimal.size();i++) {
			docIDs.add(decimal.get(i));
		}
		
		PostingList newPosting = new PostingList(termId, docIDs);
		
		//Step 5: Return the instantiated PostingList to user
		return newPosting;
	}
	
	
	public void runQueryService(String indexMode, String indexDirname) throws IOException
	{
		//Get the index reader
		try {
			Class<?> indexClass = Class.forName(indexMode+"Index");
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}
		
		//Get Index file
		File inputdir = new File(indexDirname);
		if (!inputdir.exists() || !inputdir.isDirectory()) {
			System.err.println("Invalid index directory: " + indexDirname);
			//return;
		}
		
		/* Index file */
		indexFile = new RandomAccessFile(new File(indexDirname, "corpus.index"), "r");

		String line = null;
		/* Term dictionary */
		BufferedReader termReader = new BufferedReader(new FileReader(new File(indexDirname, "term.dict")));
		while ((line = termReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			termDict.put(tokens[0], Integer.parseInt(tokens[1]));
		}
		termReader.close();

		/* Doc dictionary */
		BufferedReader docReader = new BufferedReader(new FileReader(new File(indexDirname, "doc.dict")));
		while ((line = docReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			docDict.put(Integer.parseInt(tokens[1]), tokens[0]);
		}
		docReader.close();

		/* Posting dictionary */
		BufferedReader postReader = new BufferedReader(new FileReader(new File(indexDirname, "posting.dict")));
		while ((line = postReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			posDict.put(Integer.parseInt(tokens[0]), Long.parseLong(tokens[1]));
			freqDict.put(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[2]));
		}
		postReader.close();
		
		this.running = true;
	}
    
	public List<Integer> retrieve(String query) throws IOException
	{	if(!running) 
		{
			System.err.println("Error: Query service must be initiated");
		}
		
		/*
		 * TODO: Your code here
		 *       Perform query processing with the inverted index.
		 *       return the list of IDs of the documents that match the query
		 * DONE by Earth
		 */
		List<Integer> result = new List<Integer>();
		String[] tokens = query.split(" ");	//Split the input query into tokens using white space as seperator
		
		//Step 1: Find the term ID of the tokens 
		
		Set<Integer> tokensTermID = new Set<Integer>(); //For storing the termID that will be used to retrieve the right postingList
		
		Set<String> termDictKeys = termDict.keySet();	//Get the set of keys from termDict Map

		for(String token : tokens) {
				for(String termDictKey : termDictKeys) {
					if(token.equals(termDictKey)) {
						tokensTermID.add(termDict.get(termDictKey));	//Add the termID of the token into the tokensTermID
					}
				}
		}
		
		//Step 1.5: Check first if all the terms have corresponding termID
		if(tokensTermID.size() < tokens.length) {
			System.err.print("Error: One of the search queries doesn't exist.");
		}
		
		//Step 2: Retrieve the posting lists of the tokens using termID
		List<PostingList> postingLists = new List<PostingList>(); // Set that store the postingList of the term
		
		
		for(Integer tokenID : tokensTermID) {
			private FileChannel fc = new FileChannel();
			postingLists.add(this.readPosting(fc, tokenID));
		}
		
		//Step 3: Intersect the retrieved postingLists using the intersect Helper method.
		
		int k=0;
		while(k<postingLists.size()) {
			
			if(k == 0) {
				//First iteration is intersection of postingList[0] and postingList[1] and store in result.
				result = this.intersect(postingLists.get(k), postingLists.get(k+1));
			} else {
				//After first iteration, intersect the existing result with the next postingLists[...]
				result = this.intersect(result, k+1);
			}
			k++;
		}
	
		return result;
		
	}
	
	//Helper method: Posting Lists Boolean Intersection by Earth
	
	/**
	 * This method will return the result of intersecting two posting lists.
	 * 
	 * Example: 
	 * List 1: 1 2 5 8 10
	 * List 2: 2 5 10
	 * 
	 * Result: 2 5 10
	 **/
	public List<Integer> intersect(PostingList List1, PostingList List2) {
		
		List<Integer> intersection = new List<Integer>();
		
		int pointer1 = 0;
		int pointer2 = 0;
		
		while(pointer1 < List1.getPostingLength() && pointer2 < List2.getPostingLength()) {
			if(List1.getPostingID(pointer1) == List2.getPostingID(pointer2)) {
				intersection.add(p1.getPostingID(pointer1));
				pointer1++;
				pointer2++;
			} else {
				if(List1.getPostingID(pointer1) < List2.getPostingID(pointer2)) {
					pointer1++;
				} else {
					pointer2++;
				}
			}
		}
		
		return intersection;
	}
	
    String outputQueryResult(List<Integer> res) {
        /*
         * TODO: 
         * 
         * Take the list of documents ID and prepare the search results, sorted by lexicon order. 
         * 
         * E.g.
         * 	0/fine.txt
		 *	0/hello.txt
		 *	1/bye.txt
		 *	2/fine.txt
		 *	2/hello.txt
		 *
		 * If there no matched document, output:
		 * 
		 * no results found
		 * 
         * */
    	StringBuilder outQ = new StringBuilder();
    	
    	//by benn ja (to output the query)
    	
    	if(res.size() == 0) { return "No results found";}
    	for(Integer i:res) {
    		outQ.append(docDict.get(i));
    	}
    	System.out.println(outQ);
    	return outQ.toString();
    }
	
	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 2) {
			System.err.println("Usage: java Query [Basic|VB|Gamma] index_dir");
			return;
		}

		/* Get index */
		String className = null;
		try {
			className = args[0];
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get index directory */
		String input = args[1];
		
		Query queryService = new Query();
		queryService.runQueryService(className, input);
		
		/* Processing queries */
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		/* For each query */
		String line = null;
		while ((line = br.readLine()) != null) {
			List<Integer> hitDocs = queryService.retrieve(line);
			queryService.outputQueryResult(hitDocs);
		}
		
		br.close();
	}
	
	protected void finalize()
	{
		try {
			if(indexFile != null)indexFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

