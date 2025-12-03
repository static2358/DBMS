package bdda;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Classe principale du SGBD
 * Point d'entree de l'application
 * Gere la boucle de commandes et le parsing
 */
public class SGBD {
    
    private DBConfig config;
    private DiskManager diskManager;
    private BufferManager bufferManager;
    private DBManager dbManager;
    
    // Flag pour controler la boucle principale
    private boolean running;
    
    /**
     * Constructeur
     * @param config configuration de la base de donnees
     */
    public SGBD(DBConfig config) throws IOException {
        this.config = config;
        this.diskManager = new DiskManager(config);
        this.bufferManager = new BufferManager(config, diskManager);
        this.dbManager = new DBManager(config, diskManager, bufferManager);
        this.running = true;
    }
    
    /**
     * Boucle principale de traitement des commandes
     */
    public void Run() {
        Scanner scanner = new Scanner(System.in);
        
        // Charger l'etat precedent si existant
        try {
            dbManager.LoadState();
        } catch (IOException e) {
            System.err.println("Erreur lors du chargement de l'etat : " + e.getMessage());
        }
        
        // Boucle de commandes
        while (running) {
            // Lire la commande (pas de prompt comme demande)
            String command = scanner.nextLine().trim();
            
            // Ignorer les lignes vides
            if (command.isEmpty()) {
                continue;
            }
            
            // Traiter la commande
            try {
                processCommand(command);
            } catch (Exception e) {
                System.err.println("Erreur : " + e.getMessage());
            }
        }
        
        scanner.close();
    }
    
    /**
     * Analyse et dispatch la commande vers la bonne methode
     */
    private void processCommand(String command) throws IOException {
        if (command.startsWith("CREATE TABLE ")) {
            ProcessCreateTableCommand(command);
        }
        else if (command.startsWith("DROP TABLE ") && !command.equals("DROP TABLES")) {
            ProcessDropTableCommand(command);
        }
        else if (command.equals("DROP TABLES")) {
            ProcessDropTablesCommand(command);
        }
        else if (command.startsWith("DESCRIBE TABLE ") && !command.equals("DESCRIBE TABLES")) {
            ProcessDescribeTableCommand(command);
        }
        else if (command.equals("DESCRIBE TABLES")) {
            ProcessDescribeTablesCommand(command);
        }
        else if (command.equals("EXIT")) {
            ProcessExitCommand(command);
        }
        else {
            System.out.println("Commande inconnue : " + command);
        }
    }
    
    /**
     * Traite la commande CREATE TABLE
     * Format : CREATE TABLE NomTable (Col1:Type1,Col2:Type2,...)
     * Exemple : CREATE TABLE R (X:INT,C3:FLOAT,BLA:CHAR(10))
     */
    private void ProcessCreateTableCommand(String command) throws IOException {
        // Enlever "CREATE TABLE "
        String rest = command.substring(13);
        
        // Trouver la position de la parenthese ouvrante
        int parenPos = rest.indexOf('(');
        if (parenPos == -1) {
            System.out.println("Erreur de syntaxe : parenthese manquante");
            return;
        }
        
        // Extraire le nom de la table
        String tableName = rest.substring(0, parenPos).trim();
        
        // Extraire la definition des colonnes (entre parentheses)
        String colsDef = rest.substring(parenPos + 1, rest.length() - 1);
        
        // Parser les colonnes
        List<ColumnInfo> columns = parseColumns(colsDef);
        
        // Creer la relation
        Relation relation = new Relation(tableName, columns, diskManager, bufferManager);
        
        // Ajouter au DBManager
        dbManager.AddTable(relation);
    }
    
    /**
     * Parse la definition des colonnes
     * Format : Col1:Type1,Col2:Type2,...
     */
    private List<ColumnInfo> parseColumns(String colsDef) {
        List<ColumnInfo> columns = new ArrayList<>();
        
        // Separer par virgule
        String[] colsArray = colsDef.split(",");
        
        for (String colDef : colsArray) {
            // Separer nom:type
            String[] parts = colDef.split(":");
            String colName = parts[0].trim();
            String colType = parts[1].trim();
            
            columns.add(new ColumnInfo(colName, colType));
        }
        
        return columns;
    }
    
    /**
     * Traite la commande DROP TABLE
     * Format : DROP TABLE NomTable
     */
    private void ProcessDropTableCommand(String command) throws IOException {
        // Enlever "DROP TABLE "
        String tableName = command.substring(11).trim();
        
        // Supprimer la table
        dbManager.RemoveTable(tableName);
    }
    
    /**
     * Traite la commande DROP TABLES
     * Supprime toutes les tables
     */
    private void ProcessDropTablesCommand(String command) throws IOException {
        dbManager.RemoveAllTables();
    }
    
    /**
     * Traite la commande DESCRIBE TABLE
     * Format : DESCRIBE TABLE NomTable
     */
    private void ProcessDescribeTableCommand(String command) {
        // Enlever "DESCRIBE TABLE "
        String tableName = command.substring(15).trim();
        
        // Afficher le schema
        dbManager.DescribeTable(tableName);
    }
    
    /**
     * Traite la commande DESCRIBE TABLES
     * Affiche toutes les tables
     */
    private void ProcessDescribeTablesCommand(String command) {
        dbManager.DescribeAllTables();
    }
    
    /**
     * Traite la commande EXIT
     * Sauvegarde et quitte
     */
    private void ProcessExitCommand(String command) throws IOException {
        // Sauvegarder l'etat
        dbManager.SaveState();
        
        // Flush les buffers
        bufferManager.FlushBuffers();
        
        // Fermer le DiskManager
        diskManager.finish();
        
        // Arreter la boucle
        running = false;
    }
    
    /**
     * Point d'entree de l'application
     * @param args args[0] = chemin vers le fichier de configuration
     */
    public static void main(String[] args) {
        // Verifier les arguments
        if (args.length < 1) {
            System.err.println("Usage: java SGBD <chemin_config>");
            System.err.println("Exemple: java SGBD config/config.txt");
            System.exit(1);
        }
        
        try {
            // Charger la configuration
            File configFile = new File(args[0]);
            DBConfig config = DBConfig.LoadDBConfig(configFile);
            
            // Creer et lancer le SGBD
            SGBD sgbd = new SGBD(config);
            sgbd.Run();
            
        } catch (Exception e) {
            System.err.println("Erreur fatale : " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}