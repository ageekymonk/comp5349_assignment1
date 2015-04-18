package task2;

import javafx.util.Pair;

import java.util.Comparator;

/**
 * Created by ramz on 18/04/15.
 */
public class CompareCountLocation implements Comparator<Pair<Integer, String>> {
        @Override
        public int compare(Pair<Integer, String> o1, Pair<Integer, String> o2) {
            if (o1.getKey() == o2.getKey())
            {
                return -1 * o1.getValue().compareTo(o2.getValue());
            }
            else
            {
                return o1.getKey().compareTo(o2.getKey());
            }
        }
}

