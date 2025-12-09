package bdda.storage;

import bdda.core.PageId;

/**
 * Identifiant unique d'un record dans la base de données
 * Composé du PageId de la page et de l'indice du slot dans la page
 */
public class RecordId {
    
    private PageId pageId;
    private int slotIdx;
    
    public RecordId(PageId pageId, int slotIdx) {
        this.pageId = pageId;
        this.slotIdx = slotIdx;
    }
    
    public PageId getPageId() {
        return pageId;
    }
    
    public int getSlotIdx() {
        return slotIdx;
    }
    
    @Override
    public String toString() {
        return "RecordId{page=" + pageId + ", slot=" + slotIdx + "}";
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RecordId other = (RecordId) obj;
        return slotIdx == other.slotIdx && pageId.equals(other.pageId);
    }
}
