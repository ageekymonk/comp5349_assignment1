package task1;

import java.util.Comparator;
import javafx.util.Pair;

/**
 * Created by ramz on 11/04/15.
 */
public class ComparePairByInteger implements Comparator<Pair<Integer, String>>{
    @Override
    public int compare(Pair<Integer, String> o1, Pair<Integer, String> o2) {
        if (o1.getKey() == o2.getKey())
        {
            return o1.getValue().compareTo(o2.getValue());
        }
        else
        {
            return o1.getKey().compareTo(o2.getKey());
        }
    }
}
