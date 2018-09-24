

import java.util.ArrayList;
import java.util.List;

public class PostingList {

	private int termId;
	/* A list of docIDs (i.e. postings) */
	private List<Integer> postings;

	public PostingList(int termId, List<Integer> list) {
		this.termId = termId;
		this.postings = list;
	}

	public PostingList(int termId) {
		this.termId = termId;
		this.postings = new ArrayList<Integer>();
	}

	public int getTermId() {
		return this.termId;
	}

	public List<Integer> getList() {
		return this.postings;
	}
	
	//Helper method: get the posting's length of this PostingLists by Earth :)
	public int getPostingLength() {
		return this.postings.size();
	}
	
	//Helper method: get the docID's inside List<Integer> by Earth :)
	public int getPostingID(int num) {
		return this.postings.get(num);
	}
	
	public void addDocId(int docId){
		postings.add(docId);
	}
}
