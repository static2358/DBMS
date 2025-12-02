package bdda;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Représente une relation (table) avec son schéma et son Heap File
 */
public class Relation {
    
    private String name;
    private List<ColumnInfo> columns;
    
    // TP5 : Nouveaux attributs
    private PageId headerPageId;
    private int slotCount;  // Nombre de slots par page de données
    private DiskManager diskManager;
    private BufferManager bufferManager;
    
    // Constantes pour la structure des pages
    private static final int HEADER_PAGE_SIZE = 16;  // 2 PageId = 4 * 4 bytes
    private static final int DATA_PAGE_HEADER_SIZE = 16;  // prevPage + nextPage
    
    // PageId factice pour indiquer "fin de liste"
    private static final int INVALID_PAGE_ID = -1;
    
    /**
     * Constructeur pour créer une nouvelle relation
     */
    public Relation(String name, List<ColumnInfo> columns, 
                    DiskManager diskManager, BufferManager bufferManager) throws IOException {
        this.name = name;
        this.columns = new ArrayList<>(columns);
        this.diskManager = diskManager;
        this.bufferManager = bufferManager;
        
        // Calculer le nombre de slots par page
        this.slotCount = calculateSlotCount();
        
        // Allouer la Header Page
        this.headerPageId = diskManager.allocPage();
        
        // Initialiser la Header Page (listes vides)
        initHeaderPage();
    }
    
    /**
     * Constructeur pour charger une relation existante
     */
    public Relation(String name, List<ColumnInfo> columns,
                    PageId headerPageId,
                    DiskManager diskManager, BufferManager bufferManager) {
        this.name = name;
        this.columns = new ArrayList<>(columns);
        this.headerPageId = headerPageId;
        this.diskManager = diskManager;
        this.bufferManager = bufferManager;
        this.slotCount = calculateSlotCount();
    }
    
    
    public String getName() {
        return name;
    }
    
    public List<ColumnInfo> getColumns() {
        return columns;
    }
    
    public int getColumnCount() {
        return columns.size();
    }
    
    public ColumnInfo getColumn(int index) {
        return columns.get(index);
    }

    public PageId getHeaderPageId() {
        return headerPageId;
    }
    
    public int getSlotCount() {
        return slotCount;
    }

    /**
     * Calcule la taille totale d'un record en bytes
     * @return taille en bytes
     */
    public int getRecordSize() {
        int size = 0;
        for (ColumnInfo col : columns) {
            size += col.getSizeInBytes();
        }
        return size;
    }

    private int calculateSlotCount() {
        int pageSize = diskManager.getConfig().getPageSize();
        int recordSize = getRecordSize();
        
        // Espace disponible = pageSize - header (prevPage + nextPage)
        int availableSpace = pageSize - DATA_PAGE_HEADER_SIZE;
        
        // Chaque slot = 1 record + 1 byte pour la bytemap
        int bytesPerSlot = recordSize + 1;
        
        return availableSpace / bytesPerSlot;
    }

    // ==================== HEADER PAGE ====================
    
    /**
     * Initialise la Header Page avec des listes vides
     */
    private void initHeaderPage() throws IOException {
        byte[] buffer = bufferManager.GetPage(headerPageId);
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        
        // fullPages = (-1, -1) -> liste vide
        bb.putInt(INVALID_PAGE_ID);
        bb.putInt(INVALID_PAGE_ID);
        
        // freePages = (-1, -1) -> liste vide
        bb.putInt(INVALID_PAGE_ID);
        bb.putInt(INVALID_PAGE_ID);
        
        bufferManager.FreePage(headerPageId, true);
    }

    /**
     * Lit le PageId de la première page pleine depuis la Header Page
     */
    private PageId getFullPagesHead() throws IOException {
        byte[] buffer = bufferManager.GetPage(headerPageId);
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        
        int fileIdx = bb.getInt();
        int pageIdx = bb.getInt();
        
        bufferManager.FreePage(headerPageId, false);
        
        if (fileIdx == INVALID_PAGE_ID) {
            return null;
        }
        return new PageId(fileIdx, pageIdx);
    }
    
    /**
     * Lit le PageId de la première page libre depuis la Header Page
     */
    private PageId getFreePagesHead() throws IOException {
        byte[] buffer = bufferManager.GetPage(headerPageId);
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        
        // Skip fullPages
        bb.position(8);
        
        int fileIdx = bb.getInt();
        int pageIdx = bb.getInt();
        
        bufferManager.FreePage(headerPageId, false);
        
        if (fileIdx == INVALID_PAGE_ID) {
            return null;
        }
        return new PageId(fileIdx, pageIdx);
    }
    
    /**
     * Met à jour le pointeur fullPages dans la Header Page
     */
    private void setFullPagesHead(PageId pageId) throws IOException {
        byte[] buffer = bufferManager.GetPage(headerPageId);
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        
        if (pageId == null) {
            bb.putInt(INVALID_PAGE_ID);
            bb.putInt(INVALID_PAGE_ID);
        } else {
            bb.putInt(pageId.getFileIdx());
            bb.putInt(pageId.getPageIdx());
        }
        
        bufferManager.FreePage(headerPageId, true);
    }
    
    /**
     * Met à jour le pointeur freePages dans la Header Page
     */
    private void setFreePagesHead(PageId pageId) throws IOException {
        byte[] buffer = bufferManager.GetPage(headerPageId);
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        
        // Skip fullPages
        bb.position(8);
        
        if (pageId == null) {
            bb.putInt(INVALID_PAGE_ID);
            bb.putInt(INVALID_PAGE_ID);
        } else {
            bb.putInt(pageId.getFileIdx());
            bb.putInt(pageId.getPageIdx());
        }
        
        bufferManager.FreePage(headerPageId, true);
    }

    // ==================== DATA PAGE STRUCTURE ====================
    
    /**
     * Structure d'une Data Page :
     * 
     * Offset 0-3   : prevPage.fileIdx
     * Offset 4-7   : prevPage.pageIdx
     * Offset 8-11  : nextPage.fileIdx
     * Offset 12-15 : nextPage.pageIdx
     * Offset 16... : Slots (records)
     * Fin de page  : Bytemap
     */
    
    /**
     * Calcule l'offset d'un slot dans une page de données
     */
    private int getSlotOffset(int slotIdx) {
        return DATA_PAGE_HEADER_SIZE + (slotIdx * getRecordSize());
    }
    
    /**
     * Calcule l'offset de la bytemap dans une page de données
     */
    private int getBytemapOffset() {
        return DATA_PAGE_HEADER_SIZE + (slotCount * getRecordSize());
    }
    
    /**
     * Lit le prevPage d'une Data Page
     */
    private PageId getPrevPage(ByteBuffer bb) {
        bb.position(0);
        int fileIdx = bb.getInt();
        int pageIdx = bb.getInt();
        
        if (fileIdx == INVALID_PAGE_ID) {
            return null;
        }
        return new PageId(fileIdx, pageIdx);
    }
    
    /**
     * Lit le nextPage d'une Data Page
     */
    private PageId getNextPage(ByteBuffer bb) {
        bb.position(8);
        int fileIdx = bb.getInt();
        int pageIdx = bb.getInt();
        
        if (fileIdx == INVALID_PAGE_ID) {
            return null;
        }
        return new PageId(fileIdx, pageIdx);
    }
    
    /**
     * Écrit le prevPage dans une Data Page
     */
    private void setPrevPage(ByteBuffer bb, PageId pageId) {
        bb.position(0);
        if (pageId == null) {
            bb.putInt(INVALID_PAGE_ID);
            bb.putInt(INVALID_PAGE_ID);
        } else {
            bb.putInt(pageId.getFileIdx());
            bb.putInt(pageId.getPageIdx());
        }
    }
    
    /**
     * Écrit le nextPage dans une Data Page
     */
    private void setNextPage(ByteBuffer bb, PageId pageId) {
        bb.position(8);
        if (pageId == null) {
            bb.putInt(INVALID_PAGE_ID);
            bb.putInt(INVALID_PAGE_ID);
        } else {
            bb.putInt(pageId.getFileIdx());
            bb.putInt(pageId.getPageIdx());
        }
    }
    
    /**
     * Compte le nombre de slots occupés dans une page
     */
    private int countOccupiedSlots(ByteBuffer bb) {
        int count = 0;
        int bytemapOffset = getBytemapOffset();
        
        for (int i = 0; i < slotCount; i++) {
            if (bb.get(bytemapOffset + i) == 1) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * Trouve le premier slot libre dans une page
     * Retourne -1 si aucun slot libre
     */
    private int findFreeSlot(ByteBuffer bb) {
        int bytemapOffset = getBytemapOffset();
        
        for (int i = 0; i < slotCount; i++) {
            if (bb.get(bytemapOffset + i) == 0) {
                return i;
            }
        }
        return -1;
    }
    
    /**
     * Vérifie si la page est pleine
     */
    private boolean isPageFull(ByteBuffer bb) {
        return findFreeSlot(bb) == -1;
    }
    
    /**
     * Vérifie si la page est vide
     */
    private boolean isPageEmpty(ByteBuffer bb) {
        return countOccupiedSlots(bb) == 0;
    }
        
    /**
     * Écrit un record dans le buffer à la position donnée
     * Format à taille fixe : chaque valeur est écrite sur un nombre fixe de bytes
     * 
     * @param record le record à écrire
     * @param buff le buffer (ByteBuffer)
     * @param pos la position de départ dans le buffer
     */
    public void writeRecordToBuffer(Record record, ByteBuffer buff, int pos) {
        // Vérifier que le record a le bon nombre de valeurs
        if (record.size() != columns.size()) {
            throw new IllegalArgumentException(
                "Le record a " + record.size() + " valeurs mais la relation a " + columns.size() + " colonnes");
        }
        
        // Positionner le curseur
        buff.position(pos);
        
        // Écrire chaque valeur selon son type
        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo col = columns.get(i);
            Object value = record.getValue(i);
            writeValue(buff, col, value);
        }
    }
    
    /**
     * Écrit une valeur dans le buffer selon son type
     */
    private void writeValue(ByteBuffer buff, ColumnInfo col, Object value) {
        if (col.isInt()) {
            int intValue = convertToInt(value);
            buff.putInt(intValue);
            
        } else if (col.isFloat()) {
            float floatValue = convertToFloat(value);
            buff.putFloat(floatValue);
            
        } else if (col.isChar()) {
            int maxLen = col.getMaxLength();
            String strValue = convertToString(value);
            writeFixedString(buff, strValue, maxLen);
            
        } else if (col.isVarchar()) {
            int maxLen = col.getMaxLength();
            String strValue = convertToString(value);
            writeVarcharString(buff, strValue, maxLen);
        }
    }
    
    /**
     * Écrit une chaîne de taille fixe (CHAR(T))
     * Remplit avec des espaces si la chaîne est plus courte que T
     * Tronque si la chaîne est plus longue que T
     */
    private void writeFixedString(ByteBuffer buff, String str, int maxLen) {
        // Tronquer si trop long
        if (str.length() > maxLen) {
            str = str.substring(0, maxLen);
        }
        
        // Écrire les caractères
        for (int i = 0; i < str.length(); i++) {
            buff.put((byte) str.charAt(i));
        }
        
        // Remplir avec des espaces (padding)
        for (int i = str.length(); i < maxLen; i++) {
            buff.put((byte) ' ');
        }
    }
    
    /**
     * Écrit une chaîne de taille variable (VARCHAR(T))
     * Format : 4 bytes pour la longueur réelle + T bytes pour les caractères
     * Les caractères non utilisés sont remplis avec des espaces
     */
    private void writeVarcharString(ByteBuffer buff, String str, int maxLen) {
        // Tronquer si trop long
        if (str.length() > maxLen) {
            str = str.substring(0, maxLen);
        }
        
        // Écrire la longueur réelle (4 bytes)
        buff.putInt(str.length());
        
        // Écrire les caractères
        for (int i = 0; i < str.length(); i++) {
            buff.put((byte) str.charAt(i));
        }
        
        // Remplir avec des espaces jusqu'à maxLen (padding)
        for (int i = str.length(); i < maxLen; i++) {
            buff.put((byte) ' ');
        }
    }
        
    /**
     * Lit un record depuis le buffer à la position donnée
     * Le record passé en paramètre sera rempli avec les valeurs lues
     * 
     * @param record le record à remplir (doit être vide)
     * @param buff le buffer (ByteBuffer)
     * @param pos la position de départ dans le buffer
     */
    public void readFromBuffer(Record record, ByteBuffer buff, int pos) {
        // Vider le record au cas où
        record.clear();
        
        // Positionner le curseur
        buff.position(pos);
        
        // Lire chaque valeur selon son type
        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo col = columns.get(i);
            Object value = readValue(buff, col);
            record.addValue(value);
        }
    }
    
    /**
     * Lit une valeur depuis le buffer selon son type
     */
    private Object readValue(ByteBuffer buff, ColumnInfo col) {
        if (col.isInt()) {
            return buff.getInt();
            
        } else if (col.isFloat()) {
            return buff.getFloat();
            
        } else if (col.isChar()) {
            int maxLen = col.getMaxLength();
            return readFixedString(buff, maxLen);
            
        } else if (col.isVarchar()) {
            int maxLen = col.getMaxLength();
            return readVarcharString(buff, maxLen);
        }
        
        return null;
    }
    
    /**
     * Lit une chaîne de taille fixe (CHAR(T))
     * Supprime les espaces de fin (trailing spaces)
     */
    private String readFixedString(ByteBuffer buff, int maxLen) {
        StringBuilder sb = new StringBuilder();
        
        for (int i = 0; i < maxLen; i++) {
            char c = (char) buff.get();
            sb.append(c);
        }
        
        // Supprimer les espaces de fin
        return sb.toString().stripTrailing();
    }
    
    /**
     * Lit une chaîne de taille variable (VARCHAR(T))
     * Lit d'abord la longueur, puis les caractères
     */
    private String readVarcharString(ByteBuffer buff, int maxLen) {
        // Lire la longueur réelle (4 bytes)
        int realLength = buff.getInt();
        
        StringBuilder sb = new StringBuilder();
        
        // Lire uniquement les caractères réels
        for (int i = 0; i < realLength; i++) {
            char c = (char) buff.get();
            sb.append(c);
        }
        
        // Sauter les espaces de padding
        for (int i = realLength; i < maxLen; i++) {
            buff.get(); // ignorer
        }
        
        return sb.toString();
    }
        
    /**
     * Convertit une valeur en int
     */
    private int convertToInt(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        } else if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        throw new IllegalArgumentException("Impossible de convertir en INT : " + value);
    }
    
    /**
     * Convertit une valeur en float
     */
    private float convertToFloat(Object value) {
        if (value instanceof Float) {
            return (Float) value;
        } else if (value instanceof Double) {
            return ((Double) value).floatValue();
        } else if (value instanceof String) {
            return Float.parseFloat((String) value);
        } else if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        throw new IllegalArgumentException("Impossible de convertir en FLOAT : " + value);
    }
    
    /**
     * Convertit une valeur en String
     */
    private String convertToString(Object value) {
        if (value == null) {
            return "";
        }
        return value.toString();
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Relation '").append(name).append("' (");
        sb.append(getRecordSize()).append(" bytes/record) {\n");
        for (ColumnInfo col : columns) {
            sb.append("  ").append(col).append(" (").append(col.getSizeInBytes()).append(" bytes)\n");
        }
        sb.append("}");
        return sb.toString();
    }
}