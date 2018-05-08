package IgniteConnection;

public class DbMapping {

    public String word;
    public int count;

    public DbMapping() {}

    public DbMapping(String word, int count) {
        this.word = word;
        this.count = count;
    }
    
    public String getWord(){
    	return this.word;
    }

    public int getCount(){
    	return this.count;
    }
    
    @Override
    public String toString() {
        return word + " : " + count;
    }
}
