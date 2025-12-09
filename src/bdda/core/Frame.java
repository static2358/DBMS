package bdda.core;

public class Frame {

    PageId pageId;
    byte[] buffer;
    boolean dirty;
    public int pinCount;
    public long lastAccess;

    Frame(int pageSize) {
        this.pageId = null;
        this.buffer = new byte[pageSize];
        this.dirty = false;
        this.pinCount = 0;
        this.lastAccess = System.currentTimeMillis();
    }
}
