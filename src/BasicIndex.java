import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

public class BasicIndex implements BaseIndex {

	@Override
	public PostingList readPosting(FileChannel fc) {
		/*
		 * TODO: Your code here Read and return the postings list from the given
		 * file.
		 */

		return null;
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		/*
		 * TODO: Your code here Write the given postings list to the given file.
		 */
		try {
			fc.position(fc.size());
			List<Integer> pList = p.getList();
			ByteBuffer postingBytes = ByteBuffer.allocate(2+pList.size());
			postingBytes.putInt(p.getTermId());
			postingBytes.putInt(pList.size());
			for(Integer docId:pList){
				postingBytes.putInt(docId);
			}
		} catch (IOException e) {
			System.err
				.println("Problem occured while trying to write posting"+ p.getTermId());
			throw new RuntimeException(e);
		}

	}
}
