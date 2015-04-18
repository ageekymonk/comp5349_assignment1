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
                String name1 = o1.getValue().substring(o1.getValue().lastIndexOf("/") + 1);
                String name2 = o2.getValue().substring(o2.getValue().lastIndexOf("/") + 1);

                return -1 * name1.compareTo(name2);
            }
            else
            {
                return o1.getKey().compareTo(o2.getKey());
            }
        }
}

