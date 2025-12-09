package bdda.tests;

import java.io.File;
import java.io.IOException;

import bdda.core.DBConfig;
import bdda.core.DiskManager;
import bdda.core.PageId;

/**
 * Classe de test complète pour DiskManager avec bitmap dans les fichiers
 * Teste toutes les fonctionnalités : allocation, lecture, écriture, désallocation
 */
public class DiskManagerTests {

    public static void main(String[] args) {
        try {
            System.out.println("=== TEST DISKMANAGER AVEC BITMAP ===\n");
            
            // 1. Chargement de la configuration
            testConfigLoading();
            
            // 2. Test allocation de pages
            testPageAllocation();
            
            // 3. Test écriture/lecture
            testWriteRead();
            
            // 4. Test désallocation et réutilisation
            testDeallocAndReuse();
            
            // 5. Test gestion d'erreurs
            testErrorHandling();
            
            // 6. Test Init/Finish (persistance via bitmap)
            testInitFinish();
            
            // 7. Test bitmap persistence
            testBitmapPersistence();
            
            System.out.println("\nTOUS LES TESTS REUSSIS !");
            
        } catch (Exception e) {
            System.err.println("ERREUR DANS LES TESTS : " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Test 1 : Chargement de la configuration
     */
    private static void testConfigLoading() throws IOException {
        System.out.println("1. Test chargement configuration...");
        
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        
        if (config == null) {
            throw new IOException("Configuration non chargée !");
        }
        
        System.out.println("   OK - Configuration chargée : " + config.getPath());
        System.out.println("   OK - Page size : " + config.getPageSize());
        System.out.println("   OK - Max files : " + config.getMaxFileCount());
        System.out.println("   OK - Buffer count : " + config.getBufferCount());
    }
    
    /**
     * Test 2 : Allocation de pages
     */
    private static void testPageAllocation() throws IOException {
        System.out.println("\n2. Test allocation de pages...");
        
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        cleanDBFiles(config);

        DiskManager dm = new DiskManager(config);
        
        // Allouer plusieurs pages
        PageId page1 = dm.allocPage();
        PageId page2 = dm.allocPage();
        PageId page3 = dm.allocPage();
        
        System.out.println("   OK - Page 1 allouée : (" + page1.getFileIdx() + "," + page1.getPageIdx() + ")");
        System.out.println("   OK - Page 2 allouée : (" + page2.getFileIdx() + "," + page2.getPageIdx() + ")");
        System.out.println("   OK - Page 3 allouée : (" + page3.getFileIdx() + "," + page3.getPageIdx() + ")");
        
        // Vérifier que les PageId sont différents
        if (page1.equals(page2) || page1.equals(page3) || page2.equals(page3)) {
            throw new IOException("Erreur : PageId identiques détectés !");
        }
        
        // Vérifier que les fichiers Data.bin ont bien la bitmap
        File dataFile = new File(config.getPath(), "Data0.bin");
        if (dataFile.exists()) {
            long expectedMinSize = 64 + 3 * config.getPageSize(); // bitmap + 3 pages
            if (dataFile.length() < expectedMinSize) {
                throw new IOException("Erreur : Taille fichier incorrecte !");
            }
            System.out.println("   OK - Fichier Data0.bin a la bonne structure (bitmap + pages)");
        }
        
        System.out.println("   OK - Toutes les pages ont des ID uniques");
        
        dm.finish();
    }
    
    /**
     * Test 3 : Écriture et lecture de données
     */
    private static void testWriteRead() throws IOException {
        System.out.println("\n3. Test écriture/lecture...");
        
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        cleanDBFiles(config);

        DiskManager dm = new DiskManager(config);
        
        PageId pageId = dm.allocPage();
        
        // Préparer des données à écrire
        String message = "Test bitmap!";
        byte[] dataToWrite = new byte[config.getPageSize()];
        byte[] messageBytes = message.getBytes();
        System.arraycopy(messageBytes, 0, dataToWrite, 0, 
                        Math.min(messageBytes.length, dataToWrite.length));
        
        // Écrire
        dm.WritePage(pageId, dataToWrite);
        System.out.println("   OK - Données écrites : " + message);
        
        // Lire
        byte[] dataRead = new byte[config.getPageSize()];
        dm.ReadPage(pageId, dataRead);
        
        String messageRead = new String(dataRead, 0, messageBytes.length);
        System.out.println("   OK - Données lues : " + messageRead);
        
        // Vérifier que les données sont identiques
        if (!message.equals(messageRead)) {
            throw new IOException("Erreur : Données lues différentes des données écrites !");
        }
        
        System.out.println("   OK - Écriture/lecture cohérente");

        dm.finish();
    }
    
    /**
     * Test 4 : Désallocation et réutilisation des pages
     */
    private static void testDeallocAndReuse() throws IOException {
        System.out.println("\n4. Test désallocation et réutilisation...");
        
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        cleanDBFiles(config);

        DiskManager dm = new DiskManager(config);
        
        // Allouer 3 pages
        PageId page1 = dm.allocPage();
        PageId page2 = dm.allocPage();
        PageId page3 = dm.allocPage();
        
        System.out.println("   OK - Pages allouées : (" + page1.getFileIdx() + "," + page1.getPageIdx() + ") " +
                          "(" + page2.getFileIdx() + "," + page2.getPageIdx() + ") " +
                          "(" + page3.getFileIdx() + "," + page3.getPageIdx() + ")");
        
        // Désallouer page2
        dm.DeallocPage(page2);
        System.out.println("   OK - Page (" + page2.getFileIdx() + "," + page2.getPageIdx() + ") désallouée");
        
        // Allouer une nouvelle page (devrait réutiliser page2)
        PageId reusedPage = dm.allocPage();
        System.out.println("   OK - Nouvelle page allouée : (" + reusedPage.getFileIdx() + "," + reusedPage.getPageIdx() + ")");
        
        // Vérifier que c'est page2 qui a été réutilisée
        if (!page2.equals(reusedPage)) {
            throw new IOException("Erreur : La page libérée n'a pas été réutilisée ! " +
                "Attendu: (" + page2.getFileIdx() + "," + page2.getPageIdx() + ") " +
                "Obtenu: (" + reusedPage.getFileIdx() + "," + reusedPage.getPageIdx() + ")");
        }
        
        System.out.println("   OK - Réutilisation des pages libres fonctionne (bitmap correctement mise à jour)");

        dm.finish();
    }
    
    /**
     * Test 5 : Gestion d'erreurs
     */
    private static void testErrorHandling() throws IOException {
        System.out.println("\n5. Test gestion d'erreurs...");
        
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        cleanDBFiles(config);

        DiskManager dm = new DiskManager(config);
        
        PageId validPage = dm.allocPage();
        
        // Test buffer de mauvaise taille
        try {
            byte[] wrongSizeBuffer = new byte[32];
            dm.ReadPage(validPage, wrongSizeBuffer);
            throw new IOException("Erreur : Exception attendue pour buffer de mauvaise taille !");
        } catch (IOException e) {
            if (e.getMessage().contains("Taille du buffer")) {
                System.out.println("   OK - Erreur buffer mal dimensionné détectée");
            } else {
                throw e;
            }
        }
        
        // Test lecture page inexistante
        try {
            PageId invalidPage = new PageId(0, 999);
            byte[] buffer = new byte[config.getPageSize()];
            dm.ReadPage(invalidPage, buffer);
            throw new IOException("Erreur : Exception attendue pour page inexistante !");
        } catch (IOException e) {
            if (e.getMessage().contains("inexistante")) {
                System.out.println("   OK - Erreur page inexistante détectée");
            } else {
                throw e;
            }
        }
        
        System.out.println("   OK - Gestion d'erreurs correcte");

        dm.finish();
    }
    
    /**
     * Test 6 : Persistance avec Init() et finish()
     */
    private static void testInitFinish() throws IOException {
        System.out.println("\n6. Test persistance Init/finish (bitmap dans fichier)...");
        
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        cleanDBFiles(config);
        
        PageId page1, page2, page3;
        
        // PHASE 1 : Créer des pages et en désallouer
        {
            DiskManager dm1 = new DiskManager(config);

            page1 = dm1.allocPage();
            page2 = dm1.allocPage();
            page3 = dm1.allocPage();
            
            System.out.println("   OK - Pages allouées : (" + page1.getFileIdx() + "," + page1.getPageIdx() + ") " +
                              "(" + page2.getFileIdx() + "," + page2.getPageIdx() + ") " +
                              "(" + page3.getFileIdx() + "," + page3.getPageIdx() + ")");
            
            // Désallouer page1 et page3
            dm1.DeallocPage(page1);
            dm1.DeallocPage(page3);
            
            System.out.println("   OK - Pages désallouées : (" + page1.getFileIdx() + "," + page1.getPageIdx() + ") " +
                              "(" + page3.getFileIdx() + "," + page3.getPageIdx() + ")");
            
            // La bitmap dans le fichier est mise à jour automatiquement
            dm1.finish();
            System.out.println("   OK - État sauvegardé (bitmap synchronisée dans Data.bin)");
        }
        
        // PHASE 2 : Recharger et vérifier la persistance
        {
            DiskManager dm2 = new DiskManager(config);
            System.out.println("   OK - État chargé depuis bitmap dans Data.bin");
            
            // Les pages libres devraient être réutilisées
            PageId reused1 = dm2.allocPage();
            PageId reused2 = dm2.allocPage();
            
            System.out.println("   OK - Pages réallouées : (" + reused1.getFileIdx() + "," + reused1.getPageIdx() + ") " +
                              "(" + reused2.getFileIdx() + "," + reused2.getPageIdx() + ")");
            
            // Vérifier que ce sont bien page1 et page3 qui ont été réutilisées
            boolean correct = (reused1.equals(page1) && reused2.equals(page3)) ||
                            (reused1.equals(page3) && reused2.equals(page1));
            
            if (!correct) {
                throw new IOException("Erreur : Les pages libres n'ont pas été correctement réutilisées après redémarrage !");
            }
            
            System.out.println("   OK - Bitmap persistée correctement, pages réutilisées");
            
            dm2.finish();
        }
        
        System.out.println("   OK - Persistance via bitmap fonctionnelle");
    }
    
    /**
     * Test 7 : Test spécifique de la bitmap
     */
    private static void testBitmapPersistence() throws IOException {
        System.out.println("\n7. Test persistance bitmap...");
        
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        cleanDBFiles(config);
        
        // Créer 5 pages
        {
            DiskManager dm = new DiskManager(config);
            
            for (int i = 0; i < 5; i++) {
                dm.allocPage();
            }
            
            System.out.println("   OK - 5 pages allouées");
            
            dm.finish();
        }
        
        // Vérifier que le fichier contient bien bitmap + 5 pages
        File dataFile = new File(config.getPath(), "Data0.bin");
        long expectedSize = 64 + (5 * config.getPageSize());
        
        if (dataFile.length() != expectedSize) {
            throw new IOException("Erreur : Taille fichier incorrecte ! " +
                "Attendu: " + expectedSize + " Obtenu: " + dataFile.length());
        }
        
        System.out.println("   OK - Fichier Data0.bin a la bonne taille (64 bytes bitmap + 5 pages)");
        
        // Recharger et vérifier que les 5 pages sont marquées comme utilisées
        {
            DiskManager dm = new DiskManager(config);
            
            // La prochaine allocation devrait donner la page 5 (pas de réutilisation)
            PageId newPage = dm.allocPage();
            
            if (newPage.getPageIdx() != 5) {
                throw new IOException("Erreur : Page index incorrect ! Attendu: 5 Obtenu: " + newPage.getPageIdx());
            }
            
            System.out.println("   OK - Bitmap chargée correctement, nouvelle page a l'index 5");
            
            dm.finish();
        }
        
        System.out.println("   OK - Bitmap persiste correctement entre redémarrages");
    }

    /**
     * Supprime les fichiers Data*.bin pour repartir d'un état propre
     */
    private static void cleanDBFiles(DBConfig config) {
        File dir = new File(config.getPath());
        if (!dir.exists()) {
            dir.mkdirs();
            return;
        }

        File[] files = dir.listFiles();
        if (files == null) return;

        for (File f : files) {
            String name = f.getName();
            if (name.startsWith("Data") && name.endsWith(".bin")) {
                f.delete();
            }
        }
    }
}