package org.jcl.stream;/**
 * Created by admin on 2018/9/13.
 */

/**
 * @author jichenglu
 * @create 2018-09-13 13:09
 **/
public class WordWithCount {
    public String word;
    public long count;

    public WordWithCount() {}

    public WordWithCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return word + " : " + count;
    }
}
