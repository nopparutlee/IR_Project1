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
		ByteBuffer Bbuffer = ByteBuffer.allocate(8);		//allocate for term id and document frequency
		ArrayList<Integer> docId = new ArrayList<Integer>();
		int termId = Bbuffer.getInt();
		int docFreq = Bbuffer.getInt();

		if(Bbuffer.hasRemaining() == true) {
			
			Bbuffer = ByteBuffer.allocate(docFreq*4);		//Since 1 doc id using int which have 4 bytes
			try {
				fc.read(Bbuffer);
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
			
			Bbuffer.flip();									//reset pointer again
			for(int i=0; i<docFreq; i++) {
				docId.add(Bbuffer.getInt());
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
			//fc.position(fc.size());
			//List<Integer> pList = p.getList();
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
