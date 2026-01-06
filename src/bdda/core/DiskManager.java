package bdda.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.BitSet;

public class DiskManager {
    
    private DBConfig config;

    /**
     * Bitmap d'utilisation des pages EN MÉMOIRE (cache) :
     * usedPages[fileIdx].get(pageIdx) == true  -> page utilisée (1)
     * usedPages[fileIdx].get(pageIdx) == false -> page libre (0)
     */
    private BitSet[] usedPages;

    /**
     * Taille de la bitmap en bytes au début de chaque fichier Data.bin
     * Permet de tracker jusqu'à 512 pages par fichier (64 bytes * 8 bits)
     */
    private static final int BITMAP_SIZE_BYTES = 64;
    private static final int MAX_PAGES_PER_FILE = BITMAP_SIZE_BYTES * 8; // = 512 pages

    /**
     * Constructeur du DiskManager.
     * Initialise le gestionnaire avec la configuration fournie et
     * charge les bitmaps depuis les fichiers de données.
     * 
     * @param config configuration de la base de données contenant
     *               le chemin, la taille des pages et le nombre max de fichiers
     */
    public DiskManager(DBConfig config) throws IOException {
        this.config = config;
        this.usedPages = new BitSet[config.getMaxFileCount()];
        this.Init();
    }

    /**
     * Retourne la configuration actuelle du DiskManager.
     * 
     * @return l'objet DBConfig utilisé par ce gestionnaire
     */
    public DBConfig getConfig() {
        return config;
    }
    
    /**
     * Alloue une nouvelle page pour stockage.
     * 
     * 1) Si une page précédemment désallouée (bit = 0) est disponible, elle est réutilisée.
     * 2) Sinon, une nouvelle page est créée à la fin d'un fichier existant
     *    ou dans un nouveau fichier si nécessaire.
     * 
     * @return PageId identifiant unique de la page allouée
     * @throws IOException si impossible de créer le fichier ou d'écrire la page,
     *                     ou si la limite maximale de fichiers est atteinte
     */
    public PageId allocPage() throws IOException {

        int maxFiles = config.getMaxFileCount(); 
        int pageSize = config.getPageSize();

        // 1) Essayer d'abord de réutiliser une page libre (bit = 0)
        for (int fileIdx = 0; fileIdx < maxFiles; fileIdx++) {
            File f = new File(config.getPath(), "Data" + fileIdx + ".bin");
            if (!f.exists()) {
                continue;
            }

            long length = f.length();
            if (length <= BITMAP_SIZE_BYTES) {
                continue; // Fichier vide ou juste la bitmap
            }

            int pageCount = (int) ((length - BITMAP_SIZE_BYTES) / pageSize);
            BitSet bitmap = getOrCreateBitmap(fileIdx);

            // Parcourir les pages existantes
            for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
                if (!bitmap.get(pageIdx)) {   // false -> page libre
                    bitmap.set(pageIdx);      // devient utilisée (1)
                    
                    // Synchroniser avec le fichier
                    writeBitmapToFile(f, bitmap);
                    
                    return new PageId(fileIdx, pageIdx);
                }
            }
        }
        
        // 2) Aucune page libre : rajouter une nouvelle page
        for (int fileIdx = 0; fileIdx < maxFiles; fileIdx++) {
            File f = new File(config.getPath(), "Data" + fileIdx + ".bin");
            
            if (!f.exists()) {
                createNewFileWithBitmap(f);
            }

            long length = f.length();
            int pageIdx = (int) ((length - BITMAP_SIZE_BYTES) / pageSize);

            // Vérifier qu'on ne dépasse pas la limite
            if (pageIdx >= MAX_PAGES_PER_FILE) {
                continue; // Fichier plein, essayer le suivant
            }

            try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
                // Écrit une page vide à la fin
                raf.seek(raf.length());
                raf.write(new byte[pageSize]);

                BitSet bitmap = getOrCreateBitmap(fileIdx);
                bitmap.set(pageIdx); // nouvelle page = utilisée

                // Synchroniser avec le fichier
                writeBitmapToFile(f, bitmap);

                return new PageId(fileIdx, pageIdx);
            }
        }
        
        throw new IOException("Limite de fichiers atteinte (" + maxFiles + ")");
    }

    /**
     * Désalloue une page en la marquant libre dans la bitmap (bit = 0).
     * La page pourra être réutilisée lors du prochain appel à allocPage().
     * Vérifie que la page existe avant de la désallouer.
     * 
     * @param pageId identifiant de la page à désallouer
     * @throws IOException si la page n'existe pas ou si le fichier est inaccessible
     */
    public void DeallocPage(PageId pageId) throws IOException {
        File f = getFile(pageId);
        // Vérifie que la page existe
        getOffset(pageId, f);

        BitSet bitmap = getOrCreateBitmap(pageId.getFileIdx());
        bitmap.clear(pageId.getPageIdx()); // 0 -> libre

        // Synchroniser avec le fichier
        writeBitmapToFile(f, bitmap);
    }

    /**
     * Lit le contenu d'une page et le copie dans le buffer fourni.
     * Le buffer doit avoir exactement la taille d'une page.
     * 
     * @param pageId identifiant de la page à lire
     * @param buff buffer de destination (doit faire config.getPageSize() octets)
     * @throws IOException si la page n'existe pas, le fichier est inaccessible,
     *                     ou si la taille du buffer est incorrecte
     */
    public void ReadPage(PageId pageId, byte[] buff) throws IOException {

        if (buff.length != config.getPageSize()) {
            throw new IOException("Taille du buffer (" + buff.length + 
                ") différente de la taille d'une page (" + config.getPageSize() + ")");
        }

        File f = getFile(pageId);

        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
            long offset = getOffset(pageId, f);
            raf.seek(offset);
            raf.readFully(buff);
        }
    }

    /**
     * Écrit le contenu du buffer dans la page spécifiée.
     * Le buffer doit avoir exactement la taille d'une page.
     * 
     * @param pageId identifiant de la page où écrire
     * @param buff buffer contenant les données à écrire (doit faire config.getPageSize() octets)
     * @throws IOException si la page n'existe pas, le fichier est inaccessible,
     *                     ou si la taille du buffer est incorrecte
     */
    public void WritePage(PageId pageId, byte[] buff) throws IOException {

        if (buff.length != config.getPageSize()) {
            throw new IOException("Taille du buffer (" + buff.length + 
                ") différente de la taille d'une page (" + config.getPageSize() + ")");
        }

        File f = getFile(pageId);

        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            long offset = getOffset(pageId, f);
            raf.seek(offset);
            raf.write(buff);
        }
    }

    /**
     * Finalise le DiskManager à l'arrêt du SGBD.
     * Synchronise toutes les bitmaps en mémoire vers les fichiers.
     * 
     * @throws IOException si impossible d'écrire les bitmaps
     */
    public void finish() throws IOException {
        int maxFiles = config.getMaxFileCount();

        // Sauvegarder toutes les bitmaps dans les fichiers
        for (int fileIdx = 0; fileIdx < maxFiles; fileIdx++) {
            if (usedPages[fileIdx] != null) {
                File f = new File(config.getPath(), "Data" + fileIdx + ".bin");
                if (f.exists()) {
                    writeBitmapToFile(f, usedPages[fileIdx]);
                }
            }
        }
    }

    /**
     * Initialise le DiskManager au démarrage du SGBD.
     * Crée le répertoire de données s'il n'existe pas et charge les bitmaps.
     * 
     * @throws IOException si erreur lors de la lecture des fichiers
     */
    public void Init() throws IOException {
        // Créer le répertoire de données s'il n'existe pas
        File dbDir = new File(config.getPath());
        if (!dbDir.exists()) {
            if (!dbDir.mkdirs()) {
                throw new IOException("Impossible de créer le répertoire de données : " + config.getPath());
            }
        }
        LoadState();
    }

    /**
     * Charge l'état des pages depuis les bitmaps stockées dans les fichiers Data.bin.
     */
    private void LoadState() throws IOException {
        int maxFiles = config.getMaxFileCount();

        // Charger les bitmaps DEPUIS les fichiers
        for (int fileIdx = 0; fileIdx < maxFiles; fileIdx++) {
            File f = new File(config.getPath(), "Data" + fileIdx + ".bin");
            
            if (!f.exists()) {
                usedPages[fileIdx] = new BitSet();
                continue;
            }

            // Lire la bitmap depuis le fichier
            BitSet bitmap = readBitmapFromFile(f);
            usedPages[fileIdx] = bitmap;
        }
    }

    /**
     * Crée un nouveau fichier Data.bin avec une bitmap vide au début.
     */
    private void createNewFileWithBitmap(File f) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            // Écrire bitmap vide (tous bits à 0 = toutes pages libres)
            byte[] emptyBitmap = new byte[BITMAP_SIZE_BYTES];
            raf.write(emptyBitmap);
        }
    }

    /**
     * Lit la bitmap depuis le début d'un fichier Data.bin et la convertit en BitSet.
     */
    private BitSet readBitmapFromFile(File f) throws IOException {
        BitSet bitmap = new BitSet();
        
        if (!f.exists() || f.length() < BITMAP_SIZE_BYTES) {
            return bitmap;
        }
        
        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
            raf.seek(0); // Début du fichier = bitmap
            
            byte[] bitmapBytes = new byte[BITMAP_SIZE_BYTES];
            raf.readFully(bitmapBytes);
            
            // Convertir bytes en BitSet
            for (int i = 0; i < bitmapBytes.length; i++) {
                byte b = bitmapBytes[i];
                for (int bit = 0; bit < 8; bit++) {
                    if ((b & (1 << bit)) != 0) {
                        bitmap.set(i * 8 + bit);
                    }
                }
            }
        }
        
        return bitmap;
    }

    /**
     * Écrit un BitSet dans le fichier Data.bin sous forme de bitmap.
     */
    private void writeBitmapToFile(File f, BitSet bitmap) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            raf.seek(0); // Début du fichier
            
            byte[] bitmapBytes = new byte[BITMAP_SIZE_BYTES];
            
            // Convertir BitSet en bytes
            for (int i = 0; i < MAX_PAGES_PER_FILE; i++) {
                if (bitmap.get(i)) {
                    int byteIndex = i / 8;
                    int bitIndex = i % 8;
                    bitmapBytes[byteIndex] |= (1 << bitIndex);
                }
            }
            
            raf.write(bitmapBytes);
        }
    }

    /**
     * Retourne l'objet File correspondant au PageId fourni.
     * Vérifie que le fichier existe sur le disque.
     * 
     * @param pageId identifiant de la page
     * @return objet File correspondant au fichier contenant cette page
     * @throws IOException si le fichier n'existe pas
     */
    private File getFile(PageId pageId) throws IOException {
        File f = new File(config.getPath(), "Data" + pageId.getFileIdx() + ".bin");
        
        if (!f.exists()) {
            throw new IOException("Fichier inexistant : " + f.getAbsolutePath());
        }
        
        return f;
    }

    /**
     * Calcule l'offset (position en octets) d'une page dans son fichier.
     * Prend en compte la bitmap au début du fichier.
     * Vérifie que la page existe réellement dans le fichier.
     * 
     * @param pageId identifiant de la page
     * @param f fichier contenant la page
     * @return position en octets du début de la page dans le fichier
     * @throws IOException si la page dépasse la taille actuelle du fichier
     */
    private long getOffset(PageId pageId, File f) throws IOException {
        long offset = BITMAP_SIZE_BYTES + 
                     (long) pageId.getPageIdx() * config.getPageSize();

        if (offset + config.getPageSize() > f.length()) {
            throw new IOException("Page " + pageId.getPageIdx() + 
                    " inexistante dans le fichier " + f.getName());
        }
        
        return offset;
    }

    /**
     * Récupère (ou crée) la bitmap d'un fichier donné.
     */
    private BitSet getOrCreateBitmap(int fileIdx) {
        BitSet bitmap = usedPages[fileIdx];
        if (bitmap == null) {
            bitmap = new BitSet();
            usedPages[fileIdx] = bitmap;
        }
        return bitmap;
    }

}