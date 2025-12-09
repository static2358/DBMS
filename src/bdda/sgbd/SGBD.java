package bdda.sgbd;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import bdda.core.BufferManager;
import bdda.core.DBConfig;
import bdda.core.DiskManager;
import bdda.core.PageId;
import bdda.manager.DBManager;
import bdda.query.Condition;
import bdda.query.IRecordIterator;
import bdda.query.ProjectOperator;
import bdda.query.RecordPrinter;
import bdda.query.RelationScanner;
import bdda.query.SelectOperator;
import bdda.storage.ColumnInfo;
import bdda.storage.Record;
import bdda.storage.RecordId;
import bdda.storage.Relation;

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
        else if (command.startsWith("INSERT INTO ")) {
            ProcessInsertCommand(command);
        }
        else if (command.startsWith("APPEND INTO ")) {
            ProcessAppendCommand(command);
        }
        else if (command.startsWith("SELECT ")) {
            ProcessSelectCommand(command);
        }
        else if (command.startsWith("DELETE ")) {
            ProcessDeleteCommand(command);
        }
        else if (command.startsWith("UPDATE ")) {
            ProcessUpdateCommand(command);
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

    private void ProcessInsertCommand(String command) throws IOException {
        // Enlever "INSERT INTO "
        String rest = command.substring(12);
        
        // Trouver " VALUES "
        int valuesPos = rest.indexOf(" VALUES ");
        String tableName = rest.substring(0, valuesPos).trim();
        
        // Extraire les valeurs (entre parentheses)
        String valuesPart = rest.substring(valuesPos + 8).trim();
        String valuesStr = valuesPart.substring(1, valuesPart.length() - 1);
        
        // Recuperer la relation
        Relation relation = dbManager.GetTable(tableName);
        if (relation == null) {
            System.out.println("Table inexistante : " + tableName);
            return;
        }
        
        // Parser les valeurs
        List<Object> values = parseValues(valuesStr, relation.getColumns());
        
        // Inserer le record
        Record record = new Record(values);
        relation.InsertRecord(record);
    }

    /**
     * Parse les valeurs d'un INSERT ou CSV
     */
    private List<Object> parseValues(String valuesStr, List<ColumnInfo> columns) {
        List<Object> values = new ArrayList<>();
        List<String> tokens = splitValues(valuesStr);
        
        for (int i = 0; i < tokens.size() && i < columns.size(); i++) {
            String token = tokens.get(i).trim();
            ColumnInfo col = columns.get(i);
            
            Object value = parseValue(token, col);
            values.add(value);
        }
        
        return values;
    }

    /**
     * Split les valeurs en tenant compte des guillemets
     */
    private List<String> splitValues(String valuesStr) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        
        for (int i = 0; i < valuesStr.length(); i++) {
            char c = valuesStr.charAt(i);
            
            if (c == '"') {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (c == ',' && !inQuotes) {
                tokens.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        
        return tokens;
    }

    /**
     * Parse une valeur selon son type
     */
    private Object parseValue(String token, ColumnInfo col) {
        token = token.trim();
        
        // Enlever les guillemets si present
        if (token.startsWith("\"") && token.endsWith("\"")) {
            token = token.substring(1, token.length() - 1);
        }
        
        if (col.isInt()) {
            return Integer.parseInt(token);
        } else if (col.isFloat()) {
            return Float.parseFloat(token);
        } else {
            return token;
        }
    }

    /**
     * Traite la commande APPEND INTO
     * Format : APPEND INTO nomRelation ALLRECORDS (nomFichier.csv)
     */
    private void ProcessAppendCommand(String command) throws IOException {
        // Enlever "APPEND INTO "
        String rest = command.substring(12);
        
        // Trouver " ALLRECORDS "
        int allrecordsPos = rest.indexOf(" ALLRECORDS ");
        String tableName = rest.substring(0, allrecordsPos).trim();
        
        // Extraire le nom du fichier (entre parentheses)
        String filePart = rest.substring(allrecordsPos + 12).trim();
        String fileName = filePart.substring(1, filePart.length() - 1);
        
        // Recuperer la relation
        Relation relation = dbManager.GetTable(tableName);
        if (relation == null) {
            System.out.println("Table inexistante : " + tableName);
            return;
        }
        
        // Lire le fichier CSV
        File csvFile = new File(fileName);
        if (!csvFile.exists()) {
            System.out.println("Fichier inexistant : " + fileName);
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                List<Object> values = parseValues(line, relation.getColumns());
                Record record = new Record(values);
                relation.InsertRecord(record);
            }
        }
    }

    /**
     * Traite la commande SELECT
     * Format : SELECT cols FROM nomRelation alias [WHERE conditions]
     */
    private void ProcessSelectCommand(String command) throws IOException {
        // Enlever "SELECT "
        String rest = command.substring(7);
        
        // Trouver " FROM "
        int fromPos = rest.indexOf(" FROM ");
        String selectPart = rest.substring(0, fromPos).trim();
        String afterFrom = rest.substring(fromPos + 6).trim();
        
        // Parser FROM : nomRelation alias
        String[] fromParts = afterFrom.split(" ");
        String tableName = fromParts[0];
        String alias = fromParts[1];
        
        // Recuperer la relation
        Relation relation = dbManager.GetTable(tableName);
        if (relation == null) {
            System.out.println("Table inexistante : " + tableName);
            return;
        }

        // Trouver WHERE si present
        String wherePart = null;
        int wherePos = afterFrom.indexOf(" WHERE ");
        if (wherePos >= 0) {
            wherePart = afterFrom.substring(wherePos + 7).trim();
        }
        
        // Parser les colonnes a projeter
        List<Integer> projectIndices = parseProjectColumns(selectPart, alias, relation);
        
        // Parser les conditions
        List<Condition> conditions = new ArrayList<>();
        if (wherePart != null) {
            conditions = parseConditions(wherePart, alias, relation);
        }
        
        // Creer la chaine d'iterateurs
        IRecordIterator scanner = new RelationScanner(relation, bufferManager);
        IRecordIterator selector = new SelectOperator(scanner, conditions, relation.getColumns());
        IRecordIterator projector = new ProjectOperator(selector, projectIndices);
        
        // Afficher les resultats
        RecordPrinter printer = new RecordPrinter(projector);
        int count = printer.printAll();
        
        System.out.println("Total selected records=" + count);
        
        // Fermer les iterateurs
        projector.Close();
    }

    /**
     * Parse les colonnes a projeter
     * @return liste des indices de colonnes, ou null pour SELECT *
     */
    private List<Integer> parseProjectColumns(String selectPart, String alias, Relation relation) {
        if (selectPart.equals("*")) {
            return null; // Toutes les colonnes
        }
        
        List<Integer> indices = new ArrayList<>();
        String[] cols = selectPart.split(",");
        
        for (String col : cols) {
            col = col.trim();
            // Enlever l'alias (alias.colonne -> colonne)
            if (col.startsWith(alias + ".")) {
                col = col.substring(alias.length() + 1);
            }
            
            // Trouver l'indice de la colonne
            int idx = getColumnIndex(col, relation);
            if (idx >= 0) {
                indices.add(idx);
            }
        }
        
        return indices;
    }

    /**
     * Retourne l'indice d'une colonne par son nom
     */
    private int getColumnIndex(String colName, Relation relation) {
        List<ColumnInfo> columns = relation.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(colName)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Parse les conditions WHERE
     */
    private List<Condition> parseConditions(String wherePart, String alias, Relation relation) {
        List<Condition> conditions = new ArrayList<>();
        
        // Separer par " AND "
        String[] condStrs = wherePart.split(" AND ");
        
        for (String condStr : condStrs) {
            condStr = condStr.trim();
            Condition cond = parseCondition(condStr, alias, relation);
            if (cond != null) {
                conditions.add(cond);
            }
        }
        
        return conditions;
    }

    /**
     * Parse une condition individuelle
     * Format : Terme1 OP Terme2
     */
    private Condition parseCondition(String condStr, String alias, Relation relation) {
        // Trouver l'operateur
        String operator = null;
        int opPos = -1;
        
        // Chercher les operateurs a 2 caracteres d'abord
        String[] ops2 = {"<=", ">=", "<>"};
        for (String op : ops2) {
            int pos = condStr.indexOf(op);
            if (pos > 0) {
                operator = op;
                opPos = pos;
                break;
            }
        }
        
        // Sinon chercher les operateurs a 1 caractere
        if (operator == null) {
            String[] ops1 = {"=", "<", ">"};
            for (String op : ops1) {
                int pos = condStr.indexOf(op);
                if (pos > 0) {
                    operator = op;
                    opPos = pos;
                    break;
                }
            }
        }
        
        if (operator == null) {
            return null;
        }
        
        String leftStr = condStr.substring(0, opPos).trim();
        String rightStr = condStr.substring(opPos + operator.length()).trim();
        
        // Parser le terme gauche
        int leftColIdx = -1;
        Object leftConst = null;
        if (leftStr.startsWith(alias + ".")) {
            String colName = leftStr.substring(alias.length() + 1);
            leftColIdx = getColumnIndex(colName, relation);
        } else {
            leftConst = parseConstant(leftStr, relation, leftColIdx, rightStr, alias);
        }
        
        // Parser le terme droit
        int rightColIdx = -1;
        Object rightConst = null;
        if (rightStr.startsWith(alias + ".")) {
            String colName = rightStr.substring(alias.length() + 1);
            rightColIdx = getColumnIndex(colName, relation);
        } else {
            // Determiner le type par la colonne de gauche si c'est une colonne
            rightConst = parseConstantWithType(rightStr, leftColIdx >= 0 ? relation.getColumn(leftColIdx) : null);
        }
        
        // Si gauche est une constante, determiner son type par la colonne de droite
        if (leftColIdx < 0 && leftConst == null) {
            leftConst = parseConstantWithType(leftStr, rightColIdx >= 0 ? relation.getColumn(rightColIdx) : null);
        }
        
        return new Condition(leftColIdx, leftConst, operator, rightColIdx, rightConst);
    }

    /**
     * Parse une constante (pour retrocompatibilite)
     */
    private Object parseConstant(String str, Relation relation, int otherColIdx, String otherStr, String alias) {
        // Enlever les guillemets si present
        if (str.startsWith("\"") && str.endsWith("\"")) {
            return str.substring(1, str.length() - 1);
        }
        
        // Essayer de parser comme nombre
        try {
            if (str.contains(".")) {
                return Float.parseFloat(str);
            }
            return Integer.parseInt(str);
        } catch (NumberFormatException e) {
            return str;
        }
    }

    /**
     * Parse une constante en tenant compte du type de colonne
     */
    private Object parseConstantWithType(String str, ColumnInfo col) {
        // Enlever les guillemets si present
        if (str.startsWith("\"") && str.endsWith("\"")) {
            return str.substring(1, str.length() - 1);
        }
        
        if (col != null) {
            if (col.isInt()) {
                return Integer.parseInt(str);
            } else if (col.isFloat()) {
                return Float.parseFloat(str);
            } else {
                return str;
            }
        }
        
        // Essayer de deviner le type
        try {
            if (str.contains(".")) {
                return Float.parseFloat(str);
            }
            return Integer.parseInt(str);
        } catch (NumberFormatException e) {
            return str;
        }
    }

    /**
     * Traite la commande DELETE
     * Format : DELETE nomRelation alias [WHERE conditions]
     */
    private void ProcessDeleteCommand(String command) throws IOException {
        // Enlever "DELETE "
        String rest = command.substring(7);
        
        // Parser : nomRelation alias [WHERE ...]
        String[] parts = rest.split(" ");
        String tableName = parts[0];
        String alias = parts[1];
        
        // Recuperer la relation
        Relation relation = dbManager.GetTable(tableName);
        if (relation == null) {
            System.out.println("Table inexistante : " + tableName);
            return;
        }
        
        // Trouver WHERE si present
        List<Condition> conditions = new ArrayList<>();
        int wherePos = rest.indexOf(" WHERE ");
        if (wherePos >= 0) {
            String wherePart = rest.substring(wherePos + 7).trim();
            conditions = parseConditions(wherePart, alias, relation);
        }
        
        // Parcourir et supprimer les records qui matchent
        int deleteCount = 0;
        List<RecordId> toDelete = new ArrayList<>();
        
        // D'abord, collecter les RecordIds a supprimer
        List<PageId> dataPages = relation.getDataPages();
        int slotCount = relation.getSlotCount();
        int bytemapOffset = 16 + (slotCount * relation.getRecordSize());
        
        for (PageId pageId : dataPages) {
            byte[] buffer = bufferManager.GetPage(pageId);
            java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(buffer);
            
            for (int slotIdx = 0; slotIdx < slotCount; slotIdx++) {
                if (bb.get(bytemapOffset + slotIdx) == 1) {
                    Record record = new Record();
                    int slotOffset = 16 + (slotIdx * relation.getRecordSize());
                    relation.readFromBuffer(record, bb, slotOffset);
                    
                    // Verifier les conditions
                    boolean match = true;
                    for (Condition cond : conditions) {
                        if (!cond.evaluate(record, relation.getColumns())) {
                            match = false;
                            break;
                        }
                    }
                    
                    if (match) {
                        toDelete.add(new RecordId(pageId, slotIdx));
                    }
                }
            }
            
            bufferManager.FreePage(pageId, false);
        }
        
        // Supprimer les records
        for (RecordId rid : toDelete) {
            relation.DeleteRecord(rid);
            deleteCount++;
        }
        
        System.out.println("Total deleted records=" + deleteCount);
    }

    /**
     * Traite la commande UPDATE
     * Format : UPDATE nomRelation alias SET alias.col1=val1,... [WHERE conditions]
     */
    private void ProcessUpdateCommand(String command) throws IOException {
        // Enlever "UPDATE "
        String rest = command.substring(7);
        
        // Trouver " SET "
        int setPos = rest.indexOf(" SET ");
        String beforeSet = rest.substring(0, setPos).trim();
        String afterSet = rest.substring(setPos + 5).trim();
        
        // Parser : nomRelation alias
        String[] beforeParts = beforeSet.split(" ");
        String tableName = beforeParts[0];
        String alias = beforeParts[1];
        
        // Recuperer la relation
        Relation relation = dbManager.GetTable(tableName);
        if (relation == null) {
            System.out.println("Table inexistante : " + tableName);
            return;
        }
        
        // Trouver WHERE si present
        String setPart = afterSet;
        List<Condition> conditions = new ArrayList<>();
        int wherePos = afterSet.indexOf(" WHERE ");
        if (wherePos >= 0) {
            setPart = afterSet.substring(0, wherePos).trim();
            String wherePart = afterSet.substring(wherePos + 7).trim();
            conditions = parseConditions(wherePart, alias, relation);
        }
        
        // Parser les affectations (SET)
        List<int[]> updates = new ArrayList<>(); // [colIdx]
        List<Object> newValues = new ArrayList<>();
        
        String[] assignments = setPart.split(",");
        for (String assign : assignments) {
            String[] parts = assign.split("=");
            String colPart = parts[0].trim();
            String valPart = parts[1].trim();
            
            // Enlever l'alias
            if (colPart.startsWith(alias + ".")) {
                colPart = colPart.substring(alias.length() + 1);
            }
            
            int colIdx = getColumnIndex(colPart, relation);
            ColumnInfo col = relation.getColumn(colIdx);
            Object value = parseConstantWithType(valPart, col);
            
            updates.add(new int[]{colIdx});
            newValues.add(value);
        }
        
        // Parcourir et modifier les records qui matchent
        int updateCount = 0;
        List<PageId> dataPages = relation.getDataPages();
        int slotCount = relation.getSlotCount();
        int bytemapOffset = 16 + (slotCount * relation.getRecordSize());
        
        for (PageId pageId : dataPages) {
            byte[] buffer = bufferManager.GetPage(pageId);
            java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(buffer);
            boolean pageModified = false;
            
            for (int slotIdx = 0; slotIdx < slotCount; slotIdx++) {
                if (bb.get(bytemapOffset + slotIdx) == 1) {
                    Record record = new Record();
                    int slotOffset = 16 + (slotIdx * relation.getRecordSize());
                    relation.readFromBuffer(record, bb, slotOffset);
                    
                    // Verifier les conditions
                    boolean match = true;
                    for (Condition cond : conditions) {
                        if (!cond.evaluate(record, relation.getColumns())) {
                            match = false;
                            break;
                        }
                    }
                    
                    if (match) {
                        // Appliquer les modifications
                        for (int i = 0; i < updates.size(); i++) {
                            int colIdx = updates.get(i)[0];
                            record.setValue(colIdx, newValues.get(i));
                        }
                        
                        // Reecrire le record
                        relation.writeRecordToBuffer(record, bb, slotOffset);
                        pageModified = true;
                        updateCount++;
                    }
                }
            }
            
            bufferManager.FreePage(pageId, pageModified);
        }
        
        System.out.println("Total updated records=" + updateCount);
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