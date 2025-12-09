package bdda;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

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
    
}
