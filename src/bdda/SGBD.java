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
    
    
}