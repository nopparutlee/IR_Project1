import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class BasicIndex implements BaseIndex {

	@Override
	public PostingList readPosting(FileChannel fc) {
		/*
		 * TODO: Your code here Read and return the postings list from the given
		 * file.
		 */
		ByteBuffer postingBytes = ByteBuffer.allocate(8);		//allocate for term id and document frequency
		try {
			fc.read(postingBytes);
		} catch (IOException e) {
			System.err
			.println("Problem occured while trying to read posting");
			throw new RuntimeException(e);
		}
		postingBytes.flip();
		
		if(postingBytes.hasRemaining()) {
			ArrayList<Integer> docId = new ArrayList<Integer>();
			int termId = postingBytes.getInt();
			int docFreq = postingBytes.getInt();
			
			try {
				postingBytes = ByteBuffer.allocate(docFreq*4);		//Since 1 doc id using int which have 4 bytes
				fc.read(postingBytes);
			} catch (IOException e) {
				System.err
				.println("Problem occured while trying to read posting");
			throw new RuntimeException(e);
			}
			postingBytes.flip();									//reset pointer again
			for(Integer i=0; i<docFreq; i++) {
				docId.add(postingBytes.getInt());
			}
			if(docId.isEmpty() == false) {
				return new PostingList(termId, docId);
			}
		}
		return null;
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		/*
		 * TODO: Your code here Write the given postings list to the given file.
		 */
		try {
			ByteBuffer postingBytes = ByteBuffer.allocate((2+p.getList().size())*4);
			postingBytes.putInt(p.getTermId());
			postingBytes.putInt(p.getList().size());
			for(Integer docId:p.getList()){
				postingBytes.putInt(docId);
			}
			postingBytes.flip();
			fc.write(postingBytes);
		} catch (IOException e) {
			System.err
				.println("Problem occured while trying to write posting"+ p.getTermId());
			throw new RuntimeException(e);
		}

	}
}
