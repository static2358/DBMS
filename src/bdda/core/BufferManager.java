package bdda.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BufferManager {

    private DBConfig config;
    private DiskManager diskManager;
    private BufferPolicy policy;
    private Frame[] frames;
    private Map<String, Frame> pageTable;

    public BufferManager(DBConfig config, DiskManager diskManager) {
        this.config = config;
        this.diskManager = diskManager;
        this.policy = config.getBufferPolicy();

        this.frames = new Frame[config.getBufferCount()];
        this.pageTable = new HashMap<>();

        for(int i = 0; i < config.getBufferCount(); i++) {
            frames[i] = new Frame(config.getPageSize());
        }
    }

    public DBConfig getConfig() {
        return config;
    }

    public DiskManager getDiskManager() {
        return diskManager;
    }

    public Map<String, Frame> getPageTable() {
        return pageTable;
    }

    public byte[] GetPage(PageId pageId) throws IOException {
        String key = pageId.getFileIdx() + ":" + pageId.getPageIdx();

        Frame frame = pageTable.get(key);

        if(frame != null) {
            frame.pinCount++;
            frame.lastAccess = System.currentTimeMillis();
            return frame.buffer;
        }

        Frame freeFrame = null;

        for(Frame f : frames) {
            if(f.pageId == null) {
                freeFrame = f;
                break;
            }
        }

        if (freeFrame == null) {
            
            freeFrame = selectVictimFrame(); 

            if (freeFrame == null) {
                throw new IOException("Buffer pool saturé : toutes les frames sont épinglées");
            }
            
            
            if (freeFrame.dirty) {
                diskManager.WritePage(freeFrame.pageId, freeFrame.buffer);
            }
            
            String oldKey = freeFrame.pageId.getFileIdx() + ":" + freeFrame.pageId.getPageIdx();
            pageTable.remove(oldKey);
            
        }

        diskManager.ReadPage(pageId, freeFrame.buffer);
        freeFrame.pageId = pageId;
        freeFrame.dirty = false;
        freeFrame.pinCount = 1;
        freeFrame.lastAccess = System.currentTimeMillis();
        pageTable.put(key, freeFrame);
        
        return freeFrame.buffer;
    } 

     public void SetCurrentReplacementPolicy(BufferPolicy policy) {
        if (policy == null) {
            throw new IllegalArgumentException("Politique de remplacement invalide.");
        }
        this.policy = policy;
    }

    public Frame selectVictimFrame() {
        Frame victim = null;

        if(this.policy == BufferPolicy.LRU) {
            long oldest = Long.MAX_VALUE;

            for(Frame f : frames) {
                if(f.pinCount == 0 && f.lastAccess < oldest) {
                    oldest = f.lastAccess;
                    victim = f;
                }
            }
        } else if(this.policy == BufferPolicy.MRU) {
            long newest = Long.MIN_VALUE;

            for(Frame f : frames) {
                if(f.pinCount == 0 && f.lastAccess > newest) {
                    newest = f.lastAccess;
                    victim = f;
                }
            }
        }
        return victim;
    }

    public void FreePage(PageId pageId, boolean valDirty) {
        String key = pageId.getFileIdx() + ":" + pageId.getPageIdx();
        Frame frame = pageTable.get(key);

        if (frame != null && frame.pinCount > 0) {   
            frame.pinCount--;
 
            if (valDirty) {
                frame.dirty = true;
            }
            frame.lastAccess = System.currentTimeMillis();
        }
    }


    public void FlushBuffers() throws IOException {
        
        for (Frame frame : frames) {
            if (frame.pageId != null && frame.dirty) {
                diskManager.WritePage(frame.pageId, frame.buffer);
            }
        }
        
        for (Frame frame : frames) {
            frame.pageId = null;
            frame.dirty = false;
            frame.pinCount = 0;
            frame.lastAccess = System.currentTimeMillis();
            
            for (int i = 0; i < frame.buffer.length; i++) {
                frame.buffer[i] = 0;
            }
        }
        
        pageTable.clear();
    }


}
