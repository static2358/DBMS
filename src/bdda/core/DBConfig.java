package bdda.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

enum BufferPolicy {
    LRU, MRU
}

public class DBConfig {
    private String dbpath;
    private int pagesize;
    private int dm_maxfilecount;
    private int bm_buffercount;
    private BufferPolicy bm_policy;
    
    /**
     * Constructeur complet de la classe DBConfig
     * Initialise une nouvelle configuration de base de données avec tous les paramètres
     * @param dbpath le chemin vers la base de données
     * @param pagesize la taille des pages en octets
     */
    public DBConfig(String dbpath, int pagesize, int dm_maxfilecount, int bm_buffercount, BufferPolicy bm_policy) {
        this.dbpath = dbpath;
        this.pagesize = pagesize;
        this.dm_maxfilecount = dm_maxfilecount;
        this.bm_buffercount = bm_buffercount;
        this.bm_policy = bm_policy;
    }
    
    /**
     * Récupère le chemin de la base de données
     * @return le chemin vers la base de données
     */
    public String getPath() {
        return this.dbpath;
    }

    /**
     * Récupère la taille des pages de la base de données
     * @return la taille des pages en octets
     */
    public int getPageSize() {
        return this.pagesize;
    }

    public int getMaxFileCount() {
        return this.dm_maxfilecount;
    }

    public int getBufferCount() {
        return bm_buffercount;
    }

    public BufferPolicy getBufferPolicy() {
        return bm_policy;
    }

    /**
     * Charge la configuration complète de la base de données depuis un fichier
     * @param fichier_config le fichier de configuration à lire
     * @return une nouvelle instance de DBConfig avec toutes les valeurs trouvées, ou null si incomplete
     * @throws IOException si une erreur de lecture du fichier survient
     */
    public static DBConfig LoadDBConfig(File fichier_config) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(fichier_config));
        String line  = reader.readLine();
        String dbpath = null;
        int pagesize = 0;
        int dm_maxfilecount = 0;
        int bm_buffercount = 0;
        BufferPolicy bm_Policy = null;
        
        while(line != null) {
            if(line.startsWith("dbpath = '")) {
                int start = line.indexOf("'");
                int end = line.indexOf("'", start + 1);
                dbpath = line.substring(start + 1, end);
            }
            else if (line.startsWith("pagesize = ")) {
                String value = line.substring("pagesize = ".length()).trim();
                pagesize = Integer.parseInt(value);
            }
            else if (line.startsWith("dm_maxfilecount = ")) {
                String value = line.substring("dm_maxfilecount = ".length()).trim();
                dm_maxfilecount = Integer.parseInt(value);
            } 
            else if(line.startsWith("bm_policy = '")) {
                int start = line.indexOf("'");
                int end = line.indexOf("'", start + 1);
                String bm_policyStr = line.substring(start + 1, end); 
                bm_Policy = BufferPolicy.valueOf(bm_policyStr.toUpperCase());
            }
            else if(line.startsWith("bm_buffercount = ")) {
                String value = line.substring("bm_buffercount = ".length()).trim();
                bm_buffercount = Integer.parseInt(value);
            }
            line = reader.readLine();
        }
        
        reader.close();
        if (dbpath != null && pagesize > 0 && dm_maxfilecount > 0 && bm_Policy != null && bm_buffercount > 0) {
            return new DBConfig(dbpath, pagesize, dm_maxfilecount, bm_buffercount, bm_Policy);
        }

        return null;
    }
        
}



    


