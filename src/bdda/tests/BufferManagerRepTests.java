package bdda.tests;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import bdda.core.*;


public class BufferManagerRepTests {

    public static void main(String[] args) {
        System.out.println("TEST BUFFER MANAGER - INDEX DES PAGES");
        System.out.println("====================================");
        
        try {
            testIndexPages();
            testToutesPagesEpinglees();
            System.out.println("TOUS LES TESTS PASSES !");
        } catch (Exception e) {
            System.out.println("ERREUR : " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public static void testIndexPages() throws IOException {
        System.out.println("Test avec affichage des index...");
        
        // 1. Setup
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        DiskManager dm = new DiskManager(config);
        BufferManager bm = new BufferManager(config, dm);
        
        System.out.println("Buffer count: " + config.getBufferCount());
        
        // 2. Charger 8 pages dans le buffer (index 0,0 à 0,7)
        System.out.println("\n--- CHARGEMENT 8 PAGES (0,0 à 0,7) ---");
        PageId[] pages = new PageId[12];
        
        for(int i = 0; i < 8; i++) {
            pages[i] = dm.allocPage();
            System.out.println("Page créée: (" + pages[i].getFileIdx() + "," + pages[i].getPageIdx() + ")");
            
            writePageData(dm, pages[i], "DATA_" + i, config.getPageSize());
            bm.GetPage(pages[i]);
            System.out.println("  -> Chargée dans le buffer");
        }
        
        // Afficher l'état du buffer après chargement
        System.out.println("\n=== ÉTAT BUFFER APRÈS CHARGEMENT ===");
        afficherEtatBuffer(bm);
        
        // 3. Libérer toutes les pages (pinCount = 0)
        System.out.println("\n--- LIBÉRATION DES 8 PAGES ---");
        for(int i = 0; i < 8; i++) {
            bm.FreePage(pages[i], false);
            System.out.println("Page (" + pages[i].getFileIdx() + "," + pages[i].getPageIdx() + ") libérée");
        }
        
        System.out.println("\n=== ÉTAT BUFFER APRÈS LIBÉRATION ===");
        afficherEtatBuffer(bm);
        
        // 4. Charger 4 nouvelles pages (index 0,8 à 0,11)
        System.out.println("\n--- CHARGEMENT 4 NOUVELLES PAGES (0,8 à 0,11) ---");
        
        for(int i = 8; i < 12; i++) {
            pages[i] = dm.allocPage();
            System.out.println("Nouvelle page créée: (" + pages[i].getFileIdx() + "," + pages[i].getPageIdx() + ")");
            
            writePageData(dm, pages[i], "DATA_" + i, config.getPageSize());
            bm.GetPage(pages[i]);
            System.out.println("  -> Chargée dans le buffer (remplacement LRU)");
            bm.FreePage(pages[i], false);
            
            // Afficher l'état après chaque nouveau chargement
            System.out.println("État buffer après page " + i + ":");
            afficherEtatBuffer(bm);
            System.out.println();
        }
        
        // 5. État final
        System.out.println("\n=== ÉTAT FINAL DU BUFFER ===");
        afficherEtatBuffer(bm);
        
        System.out.println("\n--- ANALYSE ---");
        System.out.println("Si LRU fonctionne correctement:");
        System.out.println("- Les 4 premières pages (0,0) (0,1) (0,2) (0,3) ont été éjectées");
        System.out.println("- Les 4 dernières pages (0,4) (0,5) (0,6) (0,7) sont restées");
        System.out.println("- Les 4 nouvelles pages (0,8) (0,9) (0,10) (0,11) sont en mémoire");
        
        System.out.println("\nTest terminé !");
    }
    
    private static void afficherEtatBuffer(BufferManager bm) {
        Map<String, Frame> pageTable = bm.getPageTable();
        
        System.out.println("Pages en mémoire (" + pageTable.size() + "/8):");
        if(pageTable.isEmpty()) {
            System.out.println("  (Aucune page en mémoire)");
        } else {
            for(String key : pageTable.keySet()) {
                Frame frame = pageTable.get(key);
                System.out.println("  " + key + " (pinCount=" + frame.pinCount + ", lastAccess=" + frame.lastAccess + ")");
            }
        }
    }
    
    private static void writePageData(DiskManager dm, PageId pageId, String data, int pageSize) throws IOException {
        byte[] pageData = new byte[pageSize];
        System.arraycopy(data.getBytes(), 0, pageData, 0, data.length());
        dm.WritePage(pageId, pageData);
    }

    public static void testToutesPagesEpinglees() throws IOException {
        System.out.println("\n\n====================================");
        System.out.println("TEST CAS TOUTES PAGES EPINGLEES");
        System.out.println("====================================");
        
        // 1. Setup nouveau
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        DiskManager dm = new DiskManager(config);
        BufferManager bm = new BufferManager(config, dm);
        
        // 2. Charger 8 pages et les GARDER épinglées
        System.out.println("--- CHARGEMENT 8 PAGES EPINGLEES ---");
        PageId[] pages = new PageId[9];
        
        for(int i = 0; i < 8; i++) {
            pages[i] = dm.allocPage();
            writePageData(dm, pages[i], "PINNED_" + i, config.getPageSize());
            bm.GetPage(pages[i]);
            System.out.println("Page (" + pages[i].getFileIdx() + "," + pages[i].getPageIdx() + ") chargée et EPINGLEE");
            // PAS DE FreePage() ! Les pages restent épinglées (pinCount = 1)
        }
        
        System.out.println("\n=== ETAT BUFFER - TOUTES EPINGLEES ===");
        afficherEtatBuffer(bm);
        
        // 3. Essayer de charger une 9ème page
        System.out.println("\n--- TENTATIVE CHARGEMENT 9EME PAGE ---");
        pages[8] = dm.allocPage();
        writePageData(dm, pages[8], "NOUVELLE_PAGE", config.getPageSize());
        
        System.out.println("Tentative de charger page (" + pages[8].getFileIdx() + "," + pages[8].getPageIdx() + ")...");
        System.out.println("Toutes les pages sont épinglées (pinCount = 1)");
        System.out.println("selectVictimFrame() ne devrait trouver aucune victime");
        System.out.println("Une IOException devrait être levée...");
        
        boolean exceptionLevee = false;
        try {
            bm.GetPage(pages[8]);
            System.out.println("ERREUR : Aucune exception levée !");
        } catch (IOException e) {
            exceptionLevee = true;
            System.out.println("Exception levée comme attendu : " + e.getMessage());
        }
        
        if (exceptionLevee) {
            System.out.println("Test réussi : Buffer saturé détecté correctement !");
        } else {
            System.out.println("Test échoué : Exception attendue non levée !");
        }
        
        System.out.println("\n=== ETAT BUFFER FINAL ===");
        afficherEtatBuffer(bm);
        
        System.out.println("\nTest épinglage terminé !");
    }
}