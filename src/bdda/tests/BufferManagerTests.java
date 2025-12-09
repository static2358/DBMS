package bdda.tests;

import java.io.File;
import java.io.IOException;

import bdda.core.BufferManager;
import bdda.core.DBConfig;
import bdda.core.DiskManager;
import bdda.core.PageId;

public class BufferManagerTests {

    public static void main(String[] args) {
        System.out.println("TEST BUFFER MANAGER - CYCLE COMPLET");
        System.out.println("=====================================");
        
        try {
            testCycleComplet();
            System.out.println("TOUS LES TESTS PASSÉS !");
        } catch (Exception e) {
            System.out.println("ERREUR : " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public static void testCycleComplet() throws IOException {
        System.out.println("Test cycle complet...");
        
        // 1. Setup
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        DiskManager dm = new DiskManager(config);
        BufferManager bm = new BufferManager(config, dm);
        
        // 2. Allouer une page
        PageId pageId = dm.allocPage();
        System.out.println("Page allouée : " + pageId.getFileIdx() + " " + pageId.getPageIdx());
        
        // 3. Écrire des données initiales sur disque
        byte[] initialData = new byte[config.getPageSize()];
        String message1 = "Hello World Initial!";
        System.arraycopy(message1.getBytes(), 0, initialData, 0, message1.length());
        dm.WritePage(pageId, initialData);
        System.out.println("Écrit sur disque : '" + message1 + "'");
        
        // 4. Premier GetPage - charge depuis le disque
        System.out.println("\n--- PREMIER GetPage ---");
        byte[] buffer1 = bm.GetPage(pageId);
        String read1 = new String(buffer1, 0, message1.length());
        System.out.println("Lu depuis buffer : '" + read1 + "'");
        System.out.println("Données lues correctement : " + read1.equals(message1));
        
        // 5. Modifier le buffer en mémoire
        System.out.println("\n--- MODIFICATION EN MÉMOIRE ---");
        String message2 = "Modified in Buffer!!";
        // Effacer l'ancien contenu
        for(int i = 0; i < 30; i++) buffer1[i] = 0;
        // Écrire le nouveau
        System.arraycopy(message2.getBytes(), 0, buffer1, 0, message2.length());
        System.out.println("Modifié en mémoire : '" + message2 + "'");
        
        // 6. Deuxième GetPage - devrait retourner le MÊME buffer modifié
        System.out.println("\n--- DEUXIÈME GetPage (même page) ---");
        byte[] buffer2 = bm.GetPage(pageId);
        String read2 = new String(buffer2, 0, message2.length());
        System.out.println("Lu depuis buffer : '" + read2 + "'");
        System.out.println("Même buffer retourné : " + (buffer1 == buffer2));
        System.out.println("Modifications préservées : " + read2.equals(message2));
        
        // 7. Libérer la page (marquée dirty)
        System.out.println("\n--- LIBÉRATION ---");
        bm.FreePage(pageId, true);
        System.out.println("Page libérée (dirty = true)");
        
        // 8. Vérifier que la page est encore en mémoire
        System.out.println("\n--- TROISIÈME GetPage (après FreePage) ---");
        byte[] buffer3 = bm.GetPage(pageId);
        String read3 = new String(buffer3, 0, message2.length());
        System.out.println("Lu depuis buffer : '" + read3 + "'");
        System.out.println("Page encore en mémoire : " + (buffer1 == buffer3));
        
        // 9. Flush - sauvegarder sur disque
        System.out.println("\n--- FLUSH BUFFERS ---");
        bm.FreePage(pageId, true); // Libérer d'abord
        bm.FlushBuffers();
        System.out.println("FlushBuffers effectué");
        
        // 10. Nouveau GetPage - devrait recharger depuis le disque
        System.out.println("\n--- QUATRIÈME GetPage (après flush) ---");
        byte[] buffer4 = bm.GetPage(pageId);
        String read4 = new String(buffer4, 0, message2.length());
        System.out.println("Lu depuis disque : '" + read4 + "'");
        System.out.println("Nouveau buffer alloué : " + (buffer1 == buffer4));
        System.out.println("Données sauvegardées : " + read4.equals(message2));
        
        // 11. Vérification finale directe sur disque
        System.out.println("\n--- VÉRIFICATION DISQUE ---");
        byte[] diskData = new byte[config.getPageSize()];
        dm.ReadPage(pageId, diskData);
        String diskRead = new String(diskData, 0, message2.length());
        System.out.println("Lu directement du disque : '" + diskRead + "'");
        System.out.println("Disque contient bien les modifications : " + diskRead.equals(message2));
        
        System.out.println("\nTest cycle complet terminé !");
    }
}
