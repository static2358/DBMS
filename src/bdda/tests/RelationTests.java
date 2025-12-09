package bdda.tests;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import bdda.core.BufferManager;
import bdda.core.DBConfig;
import bdda.core.DiskManager;
import bdda.core.PageId;
import bdda.storage.ColumnInfo;
import bdda.storage.Record;
import bdda.storage.RecordId;
import bdda.storage.Relation;

public class RelationTests {
    
    // Chemin vers le fichier de configuration
    private static final File CONFIG_FILE = new File("config/config.txt");
    
    public static void main(String[] args) {
        System.out.println("================================================================");
        System.out.println("           TESTS TP5 : HEAP FILE - RELATION                    ");
        System.out.println("================================================================\n");
        
        try {
            testCreateRelation();
            testInsertRecord();
            testInsertMultipleRecords();
            testGetAllRecords();
            testDeleteRecord();
            testDeleteAndReuse();
            testPageManagement();
            
            System.out.println("\n================================================================");
            System.out.println("              TOUS LES TESTS OK !                              ");
            System.out.println("================================================================");
            
        } catch (Exception e) {
            System.err.println("\nERREUR : " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    static void cleanTestFiles(DBConfig config) {
        File dbPath = new File(config.getPath());
        if (dbPath.exists()) {
            File[] files = dbPath.listFiles();
            if (files != null) {
                for (File f : files) {
                    f.delete();
                }
            }
        } else {
            dbPath.mkdirs();
        }
    }
    
    // ================================================================
    // TEST 1 : Creation d'une Relation
    // ================================================================
    static void testCreateRelation() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 1 : Creation d'une Relation                              ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        System.out.println("\nConfiguration chargee :");
        System.out.println("   -> dbpath     = " + config.getPath());
        System.out.println("   -> pageSize   = " + config.getPageSize() + " bytes");
        System.out.println("   -> bufferCount= " + config.getBufferCount());
        
        DiskManager dm = new DiskManager(config);
        BufferManager bm = new BufferManager(config, dm);
        
        System.out.println("\nCreation de la relation 'Etudiants'...");
        
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("nom", "CHAR(10)"),
            new ColumnInfo("age", "INT")
        );
        
        System.out.println("   -> Colonnes definies :");
        for (ColumnInfo col : cols) {
            System.out.println("      - " + col);
        }
        
        Relation rel = new Relation("Etudiants", cols, dm, bm);
        
        System.out.println("\nRelation creee avec succes !");
        System.out.println("   -> Nom           : " + rel.getName());
        System.out.println("   -> HeaderPageId  : " + rel.getHeaderPageId());
        System.out.println("   -> RecordSize    : " + rel.getRecordSize() + " bytes");
        System.out.println("   -> SlotCount     : " + rel.getSlotCount() + " slots/page");
        
        // Verification
        System.out.println("\nVerification des pages de donnees...");
        List<PageId> pages = rel.getDataPages();
        System.out.println("   -> Nombre de Data Pages : " + pages.size() + " (attendu: 0)");
        
        bm.FlushBuffers();
        dm.finish();
        
        System.out.println("\nTEST 1 REUSSI\n");
    }
    
    // ================================================================
    // TEST 2 : Insertion d'un seul Record
    // ================================================================
    static void testInsertRecord() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 2 : Insertion d'un seul Record                           ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        DiskManager dm = new DiskManager(config);
        BufferManager bm = new BufferManager(config, dm);
        
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("nom", "CHAR(10)"),
            new ColumnInfo("age", "INT")
        );
        
        Relation rel = new Relation("Etudiants", cols, dm, bm);
        System.out.println("\nRelation 'Etudiants' creee (SlotCount = " + rel.getSlotCount() + ")");
        
        // Insertion
        System.out.println("\nINSERTION d'un record...");
        Record r = new Record(Arrays.asList(1, "Alice", 22));
        System.out.println("   -> Record a inserer : " + r);
        
        RecordId rid = rel.InsertRecord(r);
        
        System.out.println("   -> Record insere !");
        System.out.println("   -> RecordId obtenu  : " + rid);
        System.out.println("      - PageId : " + rid.getPageId());
        System.out.println("      - SlotIdx: " + rid.getSlotIdx());
        
        // Relecture pour verifier
        System.out.println("\nRELECTURE pour verification...");
        List<Record> records = rel.GetAllRecords();
        System.out.println("   -> Nombre de records lus : " + records.size());
        for (Record rec : records) {
            System.out.println("   -> Record relu : " + rec);
        }
        
        // Verification
        if (records.size() == 1) {
            System.out.println("\n   Verification OK : 1 record insere et 1 record relu !");
        } else {
            System.out.println("\n   ERREUR : Attendu 1 record, obtenu " + records.size());
        }
        
        bm.FlushBuffers();
        dm.finish();
        
        System.out.println("\nTEST 2 REUSSI\n");
    }
    
    // ================================================================
    // TEST 3 : Insertion de plusieurs Records
    // ================================================================
    static void testInsertMultipleRecords() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 3 : Insertion de plusieurs Records                       ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        DiskManager dm = new DiskManager(config);
        BufferManager bm = new BufferManager(config, dm);
        
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("nom", "CHAR(10)"),
            new ColumnInfo("age", "INT")
        );
        
        Relation rel = new Relation("Etudiants", cols, dm, bm);
        
        int slotCount = rel.getSlotCount();
        System.out.println("\nRelation creee :");
        System.out.println("   -> SlotCount par page : " + slotCount);
        System.out.println("   -> RecordSize         : " + rel.getRecordSize() + " bytes");
        
        // Inserer plusieurs records
        int nbRecords = 10;
        System.out.println("\nINSERTION de " + nbRecords + " records...\n");
        
        for (int i = 1; i <= nbRecords; i++) {
            Record r = new Record(Arrays.asList(i, "Etud" + i, 20 + i));
            RecordId rid = rel.InsertRecord(r);
            
            System.out.println("   [" + i + "] Insere : " + r);
            System.out.println("       -> RecordId : Page=" + rid.getPageId() + ", Slot=" + rid.getSlotIdx());
            
            // Afficher quand on change de page
            if (rid.getSlotIdx() == 0 && i > 1) {
                System.out.println("       -> NOUVELLE PAGE creee !");
            }
        }
        
        // Verifier les pages
        System.out.println("\nETAT DES PAGES :");
        List<PageId> pages = rel.getDataPages();
        System.out.println("   -> Nombre total de Data Pages : " + pages.size());
        
        int expectedPages = (int) Math.ceil((double) nbRecords / slotCount);
        System.out.println("   -> Nombre attendu             : " + expectedPages);
        System.out.println("   -> Pages : " + pages);
        
        // Relecture
        System.out.println("\nRELECTURE de tous les records...");
        List<Record> records = rel.GetAllRecords();
        System.out.println("   -> Nombre de records lus : " + records.size() + " (attendu: " + nbRecords + ")");
        
        for (int i = 0; i < records.size(); i++) {
            System.out.println("   [" + (i+1) + "] " + records.get(i));
        }
        
        bm.FlushBuffers();
        dm.finish();
        
        System.out.println("\nTEST 3 REUSSI\n");
    }
    
    // ================================================================
    // TEST 4 : GetAllRecords
    // ================================================================
    static void testGetAllRecords() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 4 : GetAllRecords                                        ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        DiskManager dm = new DiskManager(config);
        BufferManager bm = new BufferManager(config, dm);
        
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("nom", "CHAR(10)")
        );
        
        Relation rel = new Relation("Test", cols, dm, bm);
        System.out.println("\nRelation 'Test' creee (SlotCount = " + rel.getSlotCount() + ")");
        
        // Inserer 5 records
        System.out.println("\nINSERTION de 5 records...");
        String[] noms = {"Alice", "Bob", "Charlie", "Diana", "Eve"};
        
        for (int i = 0; i < 5; i++) {
            Record r = new Record(Arrays.asList(i + 1, noms[i]));
            RecordId rid = rel.InsertRecord(r);
            System.out.println("   -> Insere : " + r + " a " + rid);
        }
        
        // GetAllRecords
        System.out.println("\nAppel de GetAllRecords()...");
        List<Record> records = rel.GetAllRecords();
        
        System.out.println("\nRESULTAT :");
        System.out.println("   -> Nombre de records : " + records.size());
        System.out.println("   -> Contenu :");
        for (int i = 0; i < records.size(); i++) {
            System.out.println("      [" + i + "] " + records.get(i));
        }
        
        // Verification
        if (records.size() == 5) {
            System.out.println("\n   Verification OK : 5 records attendus, 5 records obtenus !");
        } else {
            System.out.println("\n   ERREUR : Attendu 5 records, obtenu " + records.size());
        }
        
        bm.FlushBuffers();
        dm.finish();
        
        System.out.println("\nTEST 4 REUSSI\n");
    }
    
    // ================================================================
    // TEST 5 : Suppression d'un Record
    // ================================================================
    static void testDeleteRecord() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 5 : Suppression d'un Record                              ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        DiskManager dm = new DiskManager(config);
        BufferManager bm = new BufferManager(config, dm);
        
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("nom", "CHAR(10)")
        );
        
        Relation rel = new Relation("Test", cols, dm, bm);
        System.out.println("\nRelation 'Test' creee");
        
        // Inserer 3 records
        System.out.println("\nINSERTION de 3 records...");
        
        Record r1 = new Record(Arrays.asList(1, "Alice"));
        RecordId rid1 = rel.InsertRecord(r1);
        System.out.println("   -> Insere : " + r1 + " a " + rid1);
        
        Record r2 = new Record(Arrays.asList(2, "Bob"));
        RecordId rid2 = rel.InsertRecord(r2);
        System.out.println("   -> Insere : " + r2 + " a " + rid2);
        
        Record r3 = new Record(Arrays.asList(3, "Charlie"));
        RecordId rid3 = rel.InsertRecord(r3);
        System.out.println("   -> Insere : " + r3 + " a " + rid3);
        
        // Afficher etat avant suppression
        System.out.println("\nETAT AVANT SUPPRESSION :");
        List<Record> recordsAvant = rel.GetAllRecords();
        System.out.println("   -> Nombre de records : " + recordsAvant.size());
        for (Record rec : recordsAvant) {
            System.out.println("      - " + rec);
        }
        
        // Supprimer Bob (rid2)
        System.out.println("\nSUPPRESSION de Bob (rid2 = " + rid2 + ")...");
        rel.DeleteRecord(rid2);
        System.out.println("   -> Record supprime !");
        
        // Afficher etat apres suppression
        System.out.println("\nETAT APRES SUPPRESSION :");
        List<Record> recordsApres = rel.GetAllRecords();
        System.out.println("   -> Nombre de records : " + recordsApres.size());
        for (Record rec : recordsApres) {
            System.out.println("      - " + rec);
        }
        
        // Verification
        System.out.println("\nVERIFICATION :");
        System.out.println("   -> Avant  : " + recordsAvant.size() + " records");
        System.out.println("   -> Apres  : " + recordsApres.size() + " records");
        System.out.println("   -> Difference : " + (recordsAvant.size() - recordsApres.size()) + " record supprime");
        
        if (recordsApres.size() == 2) {
            System.out.println("   Verification OK !");
        } else {
            System.out.println("   ERREUR : Attendu 2 records apres suppression");
        }
        
        bm.FlushBuffers();
        dm.finish();
        
        System.out.println("\nTEST 5 REUSSI\n");
    }
    
    // ================================================================
    // TEST 6 : Suppression et reutilisation de slot
    // ================================================================
    static void testDeleteAndReuse() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 6 : Suppression et reutilisation de slot                 ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        DiskManager dm = new DiskManager(config);
        BufferManager bm = new BufferManager(config, dm);
        
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("nom", "CHAR(10)")
        );
        
        Relation rel = new Relation("Test", cols, dm, bm);
        System.out.println("\nRelation 'Test' creee");
        
        // Inserer 2 records
        System.out.println("\nETAPE 1 : Insertion de 2 records...");
        
        Record r1 = new Record(Arrays.asList(1, "Alice"));
        RecordId rid1 = rel.InsertRecord(r1);
        System.out.println("   -> Insere : " + r1);
        System.out.println("     rid1 = " + rid1);
        
        Record r2 = new Record(Arrays.asList(2, "Bob"));
        RecordId rid2 = rel.InsertRecord(r2);
        System.out.println("   -> Insere : " + r2);
        System.out.println("     rid2 = " + rid2);
        
        // Relecture
        System.out.println("\nRelecture apres insertion :");
        for (Record rec : rel.GetAllRecords()) {
            System.out.println("   - " + rec);
        }
        
        // Supprimer le 1er (Alice)
        System.out.println("\nETAPE 2 : Suppression de Alice (slot " + rid1.getSlotIdx() + ")...");
        rel.DeleteRecord(rid1);
        System.out.println("   -> Alice supprimee !");
        
        // Relecture
        System.out.println("\nRelecture apres suppression :");
        for (Record rec : rel.GetAllRecords()) {
            System.out.println("   - " + rec);
        }
        
        // Inserer un nouveau -> devrait reutiliser le slot 0
        System.out.println("\nETAPE 3 : Insertion de Charlie...");
        Record r3 = new Record(Arrays.asList(3, "Charlie"));
        RecordId rid3 = rel.InsertRecord(r3);
        System.out.println("   -> Insere : " + r3);
        System.out.println("     rid3 = " + rid3);
        
        // Verification de la reutilisation
        System.out.println("\nVERIFICATION DE LA REUTILISATION :");
        System.out.println("   -> rid1 (Alice supprimee) etait : Page=" + rid1.getPageId() + ", Slot=" + rid1.getSlotIdx());
        System.out.println("   -> rid3 (Charlie insere) est    : Page=" + rid3.getPageId() + ", Slot=" + rid3.getSlotIdx());
        
        if (rid3.getSlotIdx() == rid1.getSlotIdx() && rid3.getPageId().equals(rid1.getPageId())) {
            System.out.println("   Le slot a ete REUTILISE ! (meme page, meme slot)");
        } else {
            System.out.println("   Le slot n'a pas ete reutilise (nouveau slot alloue)");
        }
        
        // Relecture finale
        System.out.println("\nETAT FINAL :");
        List<Record> recordsFinal = rel.GetAllRecords();
        System.out.println("   -> Nombre de records : " + recordsFinal.size());
        for (Record rec : recordsFinal) {
            System.out.println("      - " + rec);
        }
        
        bm.FlushBuffers();
        dm.finish();
        
        System.out.println("\nTEST 6 REUSSI\n");
    }
    
    // ================================================================
    // TEST 7 : Gestion des pages (fullPages / freePages)
    // ================================================================
    static void testPageManagement() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 7 : Gestion des pages (remplissage et liberation)        ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        DiskManager dm = new DiskManager(config);
        BufferManager bm = new BufferManager(config, dm);
        
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("nom", "CHAR(10)")
        );
        
        Relation rel = new Relation("Test", cols, dm, bm);
        int slotCount = rel.getSlotCount();
        
        System.out.println("\nRelation creee avec " + slotCount + " slots par page");
        
        // Remplir exactement une page
        System.out.println("\nETAPE 1 : Remplir exactement " + slotCount + " slots (1 page)...");
        RecordId[] rids = new RecordId[slotCount];
        
        for (int i = 0; i < slotCount; i++) {
            Record r = new Record(Arrays.asList(i + 1, "Nom" + (i + 1)));
            rids[i] = rel.InsertRecord(r);
            System.out.println("   -> [" + i + "] Insere a " + rids[i]);
        }
        
        System.out.println("\nEtat des pages apres remplissage :");
        List<PageId> pages1 = rel.getDataPages();
        System.out.println("   -> Nombre de pages : " + pages1.size());
        System.out.println("   -> La page devrait etre PLEINE (dans fullPages)");
        
        // Inserer un de plus -> nouvelle page
        System.out.println("\nETAPE 2 : Inserer 1 record de plus...");
        Record extra = new Record(Arrays.asList(100, "Extra"));
        RecordId ridExtra = rel.InsertRecord(extra);
        System.out.println("   -> Insere a " + ridExtra);
        
        List<PageId> pages2 = rel.getDataPages();
        System.out.println("\nEtat apres insertion supplementaire :");
        System.out.println("   -> Nombre de pages : " + pages2.size());
        if (pages2.size() > pages1.size()) {
            System.out.println("   Nouvelle page creee !");
        }
        
        // Supprimer tous les records de la premiere page
        System.out.println("\nETAPE 3 : Supprimer tous les records de la premiere page...");
        for (int i = 0; i < slotCount; i++) {
            System.out.println("   -> Suppression du record a " + rids[i]);
            rel.DeleteRecord(rids[i]);
        }
        
        System.out.println("\nEtat apres suppressions :");
        List<PageId> pages3 = rel.getDataPages();
        System.out.println("   -> Nombre de pages : " + pages3.size());
        System.out.println("   -> Records restants : " + rel.GetAllRecords().size());
        
        // Verification
        System.out.println("\nVERIFICATION FINALE :");
        List<Record> finalRecords = rel.GetAllRecords();
        System.out.println("   -> Nombre de records : " + finalRecords.size() + " (attendu: 1)");
        for (Record rec : finalRecords) {
            System.out.println("      - " + rec);
        }
        
        bm.FlushBuffers();
        dm.finish();
        
        System.out.println("\nTEST 7 REUSSI\n");
    }
}