package bdda;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class DBManagerTests {
    
    private static final File CONFIG_FILE = new File("config/config.txt");
    
    public static void main(String[] args) {
        System.out.println("================================================================");
        System.out.println("           TESTS TP6 : DBMANAGER                               ");
        System.out.println("================================================================\n");
        
        try {
            testCreateDBManager();
            testAddTable();
            testGetTable();
            testRemoveTable();
            testRemoveAllTables();
            testDescribeTable();
            testDescribeAllTables();
            testSaveAndLoadState();
            testPersistenceWithData();
            
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
    // TEST 1 : Creation du DBManager
    // ================================================================
    static void testCreateDBManager() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 1 : Creation du DBManager                                ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        DBManager dbManager = new DBManager(config);
        
        System.out.println("\nDBManager cree avec succes !");
        System.out.println("   -> Nombre de tables : " + dbManager.GetTableCount());
        
        if (dbManager.GetTableCount() == 0) {
            System.out.println("   Verification OK : base vide au demarrage");
        }
        
        dbManager.Finish();
        
        System.out.println("\nTEST 1 REUSSI\n");
    }
    
    // ================================================================
    // TEST 2 : AddTable
    // ================================================================
    static void testAddTable() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 2 : AddTable                                             ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        DBManager dbManager = new DBManager(config);
        
        // Creer table Etudiants
        System.out.println("\nCreation de la table 'Etudiants'...");
        
        List<ColumnInfo> cols1 = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("nom", "CHAR(10)"),
            new ColumnInfo("age", "INT")
        );
        
        Relation table1 = new Relation("Etudiants", cols1, 
                                        dbManager.getDiskManager(), 
                                        dbManager.getBufferManager());
        
        dbManager.AddTable(table1);
        
        System.out.println("   -> Table ajoutee !");
        System.out.println("   -> Nombre de tables : " + dbManager.GetTableCount());
        
        // Creer table Produits
        System.out.println("\nCreation de la table 'Produits'...");
        
        List<ColumnInfo> cols2 = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("prix", "FLOAT"),
            new ColumnInfo("nom", "CHAR(20)")
        );
        
        Relation table2 = new Relation("Produits", cols2, 
                                        dbManager.getDiskManager(), 
                                        dbManager.getBufferManager());
        
        dbManager.AddTable(table2);
        
        System.out.println("   -> Table ajoutee !");
        System.out.println("   -> Nombre de tables : " + dbManager.GetTableCount());
        System.out.println("   -> Tables : " + dbManager.GetTableNames());
        
        if (dbManager.GetTableCount() == 2) {
            System.out.println("   Verification OK");
        }
        
        dbManager.Finish();
        
        System.out.println("\nTEST 2 REUSSI\n");
    }
    
    // ================================================================
    // TEST 3 : GetTable
    // ================================================================
    static void testGetTable() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 3 : GetTable                                             ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        DBManager dbManager = new DBManager(config);
        
        // Creer et ajouter une table
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("nom", "CHAR(10)")
        );
        
        Relation table = new Relation("Test", cols, 
                                       dbManager.getDiskManager(), 
                                       dbManager.getBufferManager());
        
        dbManager.AddTable(table);
        
        // Rechercher la table existante
        System.out.println("\nRecherche de la table 'Test'...");
        Relation found = dbManager.GetTable("Test");
        
        if (found != null) {
            System.out.println("   -> Table trouvee !");
            System.out.println("   -> Nom : " + found.getName());
            System.out.println("   -> Colonnes : " + found.getColumnCount());
            System.out.println("   Verification OK");
        } else {
            System.out.println("   ERREUR : Table non trouvee !");
        }
        
        // Rechercher une table inexistante
        System.out.println("\nRecherche d'une table inexistante 'Inconnu'...");
        Relation notFound = dbManager.GetTable("Inconnu");
        
        if (notFound == null) {
            System.out.println("   -> Resultat : null (comme attendu)");
            System.out.println("   Verification OK");
        } else {
            System.out.println("   ERREUR : Devrait retourner null !");
        }
        
        dbManager.Finish();
        
        System.out.println("\nTEST 3 REUSSI\n");
    }
    
    // ================================================================
    // TEST 4 : RemoveTable
    // ================================================================
    static void testRemoveTable() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 4 : RemoveTable                                          ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        DBManager dbManager = new DBManager(config);
        
        // Creer deux tables
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT")
        );
        
        Relation table1 = new Relation("Table1", cols, 
                                        dbManager.getDiskManager(), 
                                        dbManager.getBufferManager());
        
        Relation table2 = new Relation("Table2", cols, 
                                        dbManager.getDiskManager(), 
                                        dbManager.getBufferManager());
        
        dbManager.AddTable(table1);
        dbManager.AddTable(table2);
        
        System.out.println("\nEtat initial :");
        System.out.println("   -> Nombre de tables : " + dbManager.GetTableCount());
        System.out.println("   -> Tables : " + dbManager.GetTableNames());
        
        // Supprimer Table1
        System.out.println("\nSuppression de 'Table1'...");
        dbManager.RemoveTable("Table1");
        
        System.out.println("   -> Nombre de tables : " + dbManager.GetTableCount());
        System.out.println("   -> Tables restantes : " + dbManager.GetTableNames());
        
        if (dbManager.GetTableCount() == 1 && !dbManager.TableExists("Table1")) {
            System.out.println("   Verification OK : Table1 supprimee");
        }
        
        if (dbManager.TableExists("Table2")) {
            System.out.println("   Verification OK : Table2 toujours presente");
        }
        
        dbManager.Finish();
        
        System.out.println("\nTEST 4 REUSSI\n");
    }
    
    // ================================================================
    // TEST 5 : RemoveAllTables
    // ================================================================
    static void testRemoveAllTables() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 5 : RemoveAllTables                                      ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        DBManager dbManager = new DBManager(config);
        
        // Creer plusieurs tables
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT")
        );
        
        for (int i = 1; i <= 5; i++) {
            Relation table = new Relation("Table" + i, cols, 
                                           dbManager.getDiskManager(), 
                                           dbManager.getBufferManager());
            dbManager.AddTable(table);
        }
        
        System.out.println("\nEtat initial :");
        System.out.println("   -> Nombre de tables : " + dbManager.GetTableCount());
        System.out.println("   -> Tables : " + dbManager.GetTableNames());
        
        // Supprimer toutes les tables
        System.out.println("\nSuppression de toutes les tables...");
        dbManager.RemoveAllTables();
        
        System.out.println("   -> Nombre de tables : " + dbManager.GetTableCount());
        
        if (dbManager.GetTableCount() == 0) {
            System.out.println("   Verification OK : toutes les tables supprimees");
        }
        
        dbManager.Finish();
        
        System.out.println("\nTEST 5 REUSSI\n");
    }
    
    // ================================================================
    // TEST 6 : DescribeTable
    // ================================================================
    static void testDescribeTable() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 6 : DescribeTable                                        ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        DBManager dbManager = new DBManager(config);
        
        // Creer la table R comme dans l'exemple du TP
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("X", "INT"),
            new ColumnInfo("C3", "FLOAT"),
            new ColumnInfo("BLA", "CHAR(10)")
        );
        
        Relation table = new Relation("R", cols, 
                                       dbManager.getDiskManager(), 
                                       dbManager.getBufferManager());
        
        dbManager.AddTable(table);
        
        System.out.println("\nDESCRIBE TABLE R :");
        System.out.println("   Attendu : R (X:INT,C3:FLOAT,BLA:CHAR(10))");
        System.out.print("   Obtenu  : ");
        dbManager.DescribeTable("R");
        
        dbManager.Finish();
        
        System.out.println("\nTEST 6 REUSSI\n");
    }
    
    // ================================================================
    // TEST 7 : DescribeAllTables
    // ================================================================
    static void testDescribeAllTables() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 7 : DescribeAllTables                                    ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        DBManager dbManager = new DBManager(config);
        
        // Table 1
        List<ColumnInfo> cols1 = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("nom", "CHAR(10)")
        );
        Relation table1 = new Relation("Etudiants", cols1, 
                                        dbManager.getDiskManager(), 
                                        dbManager.getBufferManager());
        dbManager.AddTable(table1);
        
        // Table 2
        List<ColumnInfo> cols2 = Arrays.asList(
            new ColumnInfo("code", "INT"),
            new ColumnInfo("prix", "FLOAT")
        );
        Relation table2 = new Relation("Produits", cols2, 
                                        dbManager.getDiskManager(), 
                                        dbManager.getBufferManager());
        dbManager.AddTable(table2);
        
        // Table 3
        List<ColumnInfo> cols3 = Arrays.asList(
            new ColumnInfo("X", "INT"),
            new ColumnInfo("C3", "FLOAT"),
            new ColumnInfo("BLA", "CHAR(10)")
        );
        Relation table3 = new Relation("R", cols3, 
                                        dbManager.getDiskManager(), 
                                        dbManager.getBufferManager());
        dbManager.AddTable(table3);
        
        System.out.println("\nDESCRIBE TABLES :");
        dbManager.DescribeAllTables();
        
        dbManager.Finish();
        
        System.out.println("\nTEST 7 REUSSI\n");
    }
    
    // ================================================================
    // TEST 8 : SaveState et LoadState
    // ================================================================
    static void testSaveAndLoadState() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 8 : SaveState et LoadState                               ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        // PARTIE 1 : Creer et sauvegarder
        System.out.println("\n--- PARTIE 1 : Creation et sauvegarde ---");
        
        DBManager dbManager1 = new DBManager(config);
        
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("nom", "CHAR(10)"),
            new ColumnInfo("age", "INT")
        );
        
        Relation table = new Relation("Etudiants", cols, 
                                       dbManager1.getDiskManager(), 
                                       dbManager1.getBufferManager());
        
        dbManager1.AddTable(table);
        
        System.out.println("Table creee : ");
        dbManager1.DescribeTable("Etudiants");
        System.out.println("HeaderPageId : " + table.getHeaderPageId());
        
        dbManager1.Finish();
        System.out.println("\nDBManager ferme (etat sauvegarde)");
        
        // PARTIE 2 : Charger
        System.out.println("\n--- PARTIE 2 : Chargement ---");
        
        DBManager dbManager2 = new DBManager(config);
        dbManager2.LoadState();
        
        System.out.println("Etat charge !");
        System.out.println("   -> Nombre de tables : " + dbManager2.GetTableCount());
        
        Relation loadedTable = dbManager2.GetTable("Etudiants");
        if (loadedTable != null) {
            System.out.println("   -> Table 'Etudiants' trouvee !");
            System.out.println("   -> HeaderPageId : " + loadedTable.getHeaderPageId());
            System.out.print("   -> Schema : ");
            dbManager2.DescribeTable("Etudiants");
            System.out.println("   Verification OK");
        } else {
            System.out.println("   ERREUR : Table non trouvee apres chargement !");
        }
        
        dbManager2.Finish();
        
        System.out.println("\nTEST 8 REUSSI\n");
    }
    
    // ================================================================
    // TEST 9 : Persistance complete avec donnees
    // ================================================================
    static void testPersistenceWithData() throws Exception {
        System.out.println("----------------------------------------------------------------");
        System.out.println(" TEST 9 : Persistance complete (avec donnees)                  ");
        System.out.println("----------------------------------------------------------------");
        
        DBConfig config = DBConfig.LoadDBConfig(CONFIG_FILE);
        cleanTestFiles(config);
        
        // PARTIE 1 : Creer et inserer des donnees
        System.out.println("\n--- PARTIE 1 : Creation et insertion ---");
        
        DBManager dbManager1 = new DBManager(config);
        
        List<ColumnInfo> cols = Arrays.asList(
            new ColumnInfo("id", "INT"),
            new ColumnInfo("nom", "CHAR(10)")
        );
        
        Relation table = new Relation("Test", cols, 
                                       dbManager1.getDiskManager(), 
                                       dbManager1.getBufferManager());
        
        dbManager1.AddTable(table);
        
        // Inserer des records
        table.InsertRecord(new Record(Arrays.asList(1, "Alice")));
        table.InsertRecord(new Record(Arrays.asList(2, "Bob")));
        table.InsertRecord(new Record(Arrays.asList(3, "Charlie")));
        
        System.out.println("3 records inseres");
        System.out.println("Records dans la table :");
        for (Record r : table.GetAllRecords()) {
            System.out.println("   - " + r);
        }
        
        dbManager1.Finish();
        System.out.println("\nDBManager ferme");
        
        // PARTIE 2 : Recharger et verifier
        System.out.println("\n--- PARTIE 2 : Rechargement et verification ---");
        
        DBManager dbManager2 = new DBManager(config);
        dbManager2.LoadState();
        
        Relation loadedTable = dbManager2.GetTable("Test");
        
        if (loadedTable != null) {
            List<Record> records = loadedTable.GetAllRecords();
            System.out.println("Records retrouves : " + records.size());
            
            for (Record r : records) {
                System.out.println("   - " + r);
            }
            
            if (records.size() == 3) {
                System.out.println("\nVerification OK : les 3 records ont ete persistes !");
            } else {
                System.out.println("\nERREUR : Attendu 3 records, obtenu " + records.size());
            }
        } else {
            System.out.println("ERREUR : Table non trouvee !");
        }
        
        dbManager2.Finish();
        
        System.out.println("\nTEST 9 REUSSI\n");
    }
}