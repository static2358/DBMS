package bdda.manager;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import bdda.core.BufferManager;
import bdda.core.DBConfig;
import bdda.core.DiskManager;
import bdda.core.PageId;
import bdda.storage.ColumnInfo;
import bdda.storage.Relation;

/**
 * Gestionnaire de la base de donnees
 * Gere toutes les relations (tables) de la base
 */
public class DBManager {
    
    private DBConfig config;
    private DiskManager diskManager;
    private BufferManager bufferManager;
    
    // Structure pour stocker les relations (nom -> Relation)
    private Map<String, Relation> tables;
    
    // Nom du fichier de sauvegarde
    private static final String SAVE_FILE = "database.save";
    
    /**
     * Constructeur
     * @param config configuration de la base de donnees
     */
    public DBManager(DBConfig config) throws IOException {
        this.config = config;
        this.diskManager = new DiskManager(config);
        this.bufferManager = new BufferManager(config, diskManager);
        this.tables = new HashMap<>();
    }

    /**
     * Constructeur - utilise les instances fournies
     * @param config configuration de la base de donnees
     * @param diskManager instance de DiskManager
     * @param bufferManager instance de BufferManager
     */
    public DBManager(DBConfig config, DiskManager diskManager, BufferManager bufferManager) {
        this.config = config;
        this.diskManager = diskManager;
        this.bufferManager = bufferManager;
        this.tables = new HashMap<>();
    }
    
    /**
     * Retourne le DiskManager
     */
    public DiskManager getDiskManager() {
        return diskManager;
    }
    
    /**
     * Retourne le BufferManager
     */
    public BufferManager getBufferManager() {
        return bufferManager;
    }
    
    /**
     * Retourne la configuration
     */
    public DBConfig getConfig() {
        return config;
    }
    

    /**
     * Ajoute une table a la base de donnees
     * @param tab la relation a ajouter
     */
    public void AddTable(Relation tab) {
        tables.put(tab.getName(), tab);
    }
    
    /**
     * Retourne une table par son nom
     * @param nomTable le nom de la table
     * @return la relation correspondante ou null si inexistante
     */
    public Relation GetTable(String nomTable) {
        return tables.get(nomTable);
    }
    
    /**
     * Supprime une table de la base de donnees
     * @param nomTable le nom de la table a supprimer
     */
    public void RemoveTable(String nomTable) throws IOException {
        Relation table = tables.get(nomTable);
        
        if (table != null) {
            // Supprimer toutes les pages de donnees de la relation
            List<PageId> dataPages = table.getDataPages();
            for (PageId pageId : dataPages) {
                diskManager.DeallocPage(pageId);
            }
            
            // Supprimer la header page
            diskManager.DeallocPage(table.getHeaderPageId());
            
            // Retirer de la map
            tables.remove(nomTable);
        }
    }
    
    /**
     * Supprime toutes les tables de la base de donnees
     */
    public void RemoveAllTables() throws IOException {
        // Creer une copie des noms pour eviter ConcurrentModificationException
        List<String> tableNames = new ArrayList<>(tables.keySet());
        
        for (String name : tableNames) {
            RemoveTable(name);
        }
    }
    
    /**
     * Affiche le schema d'une table
     * Format : NomTable (Col1:Type1,Col2:Type2,...)
     * @param nomTable le nom de la table
     */
    public void DescribeTable(String nomTable) {
        Relation table = tables.get(nomTable);
        
        if (table != null) {
            System.out.println(formatTableSchema(table));
        }
    }
    
    /**
     * Affiche les schemas de toutes les tables
     */
    public void DescribeAllTables() {
        for (Relation table : tables.values()) {
            System.out.println(formatTableSchema(table));
        }
    }
    
    /**
     * Formate le schema d'une table selon le format demande
     * Format : NomTable (Col1:Type1,Col2:Type2,...)
     */
    private String formatTableSchema(Relation table) {
        StringBuilder sb = new StringBuilder();
        sb.append(table.getName()).append(" (");
        
        List<ColumnInfo> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo col = columns.get(i);
            sb.append(col.getName()).append(":").append(col.getType());
            
            if (i < columns.size() - 1) {
                sb.append(",");
            }
        }
        
        sb.append(")");
        return sb.toString();
    }
    
    
    /**
     * Sauvegarde l'etat de la base de donnees
     * Format du fichier :
     * - Nombre de tables (int)
     * - Pour chaque table :
     *   - Nom de la table (String)
     *   - HeaderPageId.fileIdx (int)
     *   - HeaderPageId.pageIdx (int)
     *   - Nombre de colonnes (int)
     *   - Pour chaque colonne :
     *     - Nom de la colonne (String)
     *     - Type de la colonne (String)
     */
    public void SaveState() throws IOException {
        // Flush les buffers avant de sauvegarder
        bufferManager.FlushBuffers();
        
        String savePath = config.getPath() + File.separator + SAVE_FILE;
        
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(savePath))) {
            // Nombre de tables
            dos.writeInt(tables.size());
            
            // Pour chaque table
            for (Relation table : tables.values()) {
                // Nom de la table
                dos.writeUTF(table.getName());
                
                // HeaderPageId
                dos.writeInt(table.getHeaderPageId().getFileIdx());
                dos.writeInt(table.getHeaderPageId().getPageIdx());
                
                // Colonnes
                List<ColumnInfo> columns = table.getColumns();
                dos.writeInt(columns.size());
                
                for (ColumnInfo col : columns) {
                    dos.writeUTF(col.getName());
                    dos.writeUTF(col.getType());
                }
            }
        }
    }
    
    /**
     * Charge l'etat de la base de donnees
     */
    public void LoadState() throws IOException {
        String savePath = config.getPath() + File.separator + SAVE_FILE;
        File saveFile = new File(savePath);
        
        // Si le fichier n'existe pas, rien a charger
        if (!saveFile.exists()) {
            return;
        }
        
        try (DataInputStream dis = new DataInputStream(new FileInputStream(savePath))) {
            // Nombre de tables
            int nbTables = dis.readInt();
            
            // Pour chaque table
            for (int i = 0; i < nbTables; i++) {
                // Nom de la table
                String name = dis.readUTF();
                
                // HeaderPageId
                int fileIdx = dis.readInt();
                int pageIdx = dis.readInt();
                PageId headerPageId = new PageId(fileIdx, pageIdx);
                
                // Colonnes
                int nbColumns = dis.readInt();
                List<ColumnInfo> columns = new ArrayList<>();
                
                for (int j = 0; j < nbColumns; j++) {
                    String colName = dis.readUTF();
                    String colType = dis.readUTF();
                    columns.add(new ColumnInfo(colName, colType));
                }
                
                // Recreer la relation avec le constructeur pour relation existante
                Relation table = new Relation(name, columns, headerPageId, diskManager, bufferManager);
                tables.put(name, table);
            }
        }
    }
    
    /**
     * Retourne la liste des noms de toutes les tables
     */
    public List<String> GetTableNames() {
        return new ArrayList<>(tables.keySet());
    }
    
    /**
     * Verifie si une table existe
     */
    public boolean TableExists(String nomTable) {
        return tables.containsKey(nomTable);
    }
    
    /**
     * Retourne le nombre de tables
     */
    public int GetTableCount() {
        return tables.size();
    }
    
    /**
     * Termine proprement le DBManager
     * Sauvegarde l'etat et ferme les ressources
     */
    public void Finish() throws IOException {
        SaveState();
        bufferManager.FlushBuffers();
        diskManager.finish();
    }
}