package bdda.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import bdda.core.BufferManager;
import bdda.core.PageId;
import bdda.storage.Record;
import bdda.storage.Relation;

/**
 * Iterateur qui parcourt tous les records d'une relation
 * Implementation efficace : ne garde qu'un record a la fois en memoire
 */
public class RelationScanner implements IRecordIterator {
    
    private Relation relation;
    private BufferManager bufferManager;
    
    // Liste des pages de donnees
    private List<PageId> dataPages;
    private int currentPageIndex;
    private int currentSlotIndex;
    
    // Page courante en memoire
    private PageId currentPageId;
    private byte[] currentBuffer;
    
    // Constantes
    private static final int DATA_PAGE_HEADER_SIZE = 16;
    
    public RelationScanner(Relation relation, BufferManager bufferManager) throws IOException {
        this.relation = relation;
        this.bufferManager = bufferManager;
        this.dataPages = relation.getDataPages();
        this.currentPageIndex = 0;
        this.currentSlotIndex = 0;
        this.currentPageId = null;
        this.currentBuffer = null;
    }

    @Override
    public Record GetNextRecord() throws IOException {
        int slotCount = relation.getSlotCount();
        int bytemapOffset = DATA_PAGE_HEADER_SIZE + (slotCount * relation.getRecordSize());
        
        while (currentPageIndex < dataPages.size()) {
            // Charger la page si necessaire
            if (currentPageId == null || !currentPageId.equals(dataPages.get(currentPageIndex))) {
                // Liberer l'ancienne page
                if (currentPageId != null) {
                    bufferManager.FreePage(currentPageId, false);
                }
                
                currentPageId = dataPages.get(currentPageIndex);
                currentBuffer = bufferManager.GetPage(currentPageId);
            }
            
            ByteBuffer bb = ByteBuffer.wrap(currentBuffer);
            
            // Chercher le prochain slot occupe
            while (currentSlotIndex < slotCount) {
                if (bb.get(bytemapOffset + currentSlotIndex) == 1) {
                    // Slot occupe : lire le record
                    Record record = new Record();
                    int slotOffset = DATA_PAGE_HEADER_SIZE + (currentSlotIndex * relation.getRecordSize());
                    relation.readFromBuffer(record, bb, slotOffset);
                    
                    currentSlotIndex++;
                    return record;
                }
                currentSlotIndex++;
            }
            
            // Page terminee, passer a la suivante
            currentSlotIndex = 0;
            currentPageIndex++;
        }
        
        return null; // Plus de records
    }

     @Override
    public void Close() {
        if (currentPageId != null) {
            bufferManager.FreePage(currentPageId, false);
            currentPageId = null;
            currentBuffer = null;
        }
    }

    @Override
    public void Reset() throws IOException {
        Close();
        this.dataPages = relation.getDataPages();
        this.currentPageIndex = 0;
        this.currentSlotIndex = 0;
    }
    
}
