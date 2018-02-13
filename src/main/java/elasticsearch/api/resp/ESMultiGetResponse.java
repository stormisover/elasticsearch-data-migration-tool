package elasticsearch.api.resp;

import java.util.ArrayList;

/**
 * Created by I319603 on 11/14/2016.
 */
public class ESMultiGetResponse {
    private ArrayList<ESGetByIdResponse> docs;

    public ArrayList<ESGetByIdResponse> getDocs() {
        return docs;
    }

    public void setDocs(ArrayList<ESGetByIdResponse> docs) {
        this.docs = docs;
    }

    @Override
    public String toString() {
        return "ESMultiGetResponse{" +
                "docs=" + docs +
                '}';
    }
}
