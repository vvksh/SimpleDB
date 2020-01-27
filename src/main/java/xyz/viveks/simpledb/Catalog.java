package xyz.viveks.simpledb;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import lombok.NonNull;

/**
 * The Catalog keeps track of all available tables in the database and their associated schemas. For
 * now, this is a stub catalog that must be populated with tables by a user program before it can be
 * used -- eventually, this should be converted to a catalog that reads a catalog table from
 * disk. @Threadsafe
 */
public class Catalog {
  // store db file and primaryKey
  private Map<Integer, TableInfo> tableIdToDBMap;
  private BiMap<String, Integer> nameTotableIdMap;

  /** Constructor. Creates a new, empty catalog. */
  public Catalog() {
    tableIdToDBMap = new HashMap<>();
    nameTotableIdMap = HashBiMap.create();
  }

  /**
   * Add a new table to the catalog. This table's contents are stored in the specified DbFile.
   *
   * @param file the contents of the table to add; file.getId() is the identfier of this
   *     file/tupledesc param for the calls getTupleDesc and getFile
   * @param name the name of the table -- may be an empty string. May not be null. If a name
   *     conflict exists, use the last table to be added as the table for a given name.
   * @param pkeyField the name of the primary key field
   */
  public void addTable(DbFile file, String name, String pkeyField) {
    Preconditions.checkNotNull(file);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(pkeyField);

    if (nameTotableIdMap.containsKey(name)) {
      // TODO what to do with exisiting table
      System.out.println("duplicate table found");
    }
    nameTotableIdMap.forcePut(name, file.getId());
    tableIdToDBMap.put(file.getId(), new TableInfo(file, pkeyField));
  }

  public void addTable(DbFile file, String name) {
    addTable(file, name, "");
  }

  /**
   * Add a new table to the catalog. This table has tuples formatted using the specified TupleDesc
   * and its contents are stored in the specified DbFile.
   *
   * @param file the contents of the table to add; file.getId() is the identfier of this
   *     file/tupledesc param for the calls getTupleDesc and getFile
   */
  public void addTable(DbFile file) {
    addTable(file, (UUID.randomUUID()).toString());
  }

  /**
   * Return the id of the table with a specified name,
   *
   * @throws NoSuchElementException if the table doesn't exist
   */
  public int getTableId(String name) throws NoSuchElementException {
    // some code goes here
    if (nameTotableIdMap.containsKey(name)) {
      return nameTotableIdMap.get(name);
    }
    throw new NoSuchElementException("table name doesn't exist");
  }

  /**
   * Returns the tuple descriptor (schema) of the specified table
   *
   * @param tableid The id of the table, as specified by the DbFile.getId() function passed to
   *     addTable
   * @throws NoSuchElementException if the table doesn't exist
   */
  public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
    if (tableIdToDBMap.containsKey(tableid)) {
      return tableIdToDBMap.get(tableid).dbFile.getTupleDesc();
    }
    throw new NoSuchElementException(
        String.format("table with tableid: %d doesn't exist.", tableid));
  }

  /**
   * Returns the DbFile that can be used to read the contents of the specified table.
   *
   * @param tableid The id of the table, as specified by the DbFile.getId() function passed to
   *     addTable
   */
  public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
    if (tableIdToDBMap.containsKey(tableid)) {
      return tableIdToDBMap.get(tableid).dbFile;
    }
    throw new NoSuchElementException(
        String.format("table with  tableid %d doesn't exist.", tableid));
  }

  public String getPrimaryKey(int tableid) {
    if (tableIdToDBMap.containsKey(tableid)) {
      return tableIdToDBMap.get(tableid).primaryKey;
    }
    throw new NoSuchElementException(
        String.format("table with  tableid %d doesn't exist.", tableid));
  }

  public Iterator<Integer> tableIdIterator() {
    return tableIdToDBMap.keySet().iterator();
  }

  public String getTableName(int id) {
    if (nameTotableIdMap.inverse().containsKey(id)) {
      return nameTotableIdMap.inverse().get(id);
    }
    throw new NoSuchElementException(String.format("table with  tableid %d doesn't exist.", id));
  }

  /** Delete all tables from the catalog */
  public void clear() {
    this.nameTotableIdMap.clear();
    this.tableIdToDBMap.clear();
  }

  /**
   * Reads the schema from a file and creates the appropriate tables in the database.
   *
   * @param catalogFile
   */
  public void loadSchema(String catalogFile) {
    String line = "";
    String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
    try {
      BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

      while ((line = br.readLine()) != null) {
        // assume line is of the format name (field type, field type, ...)
        String name = line.substring(0, line.indexOf("(")).trim();
        // System.out.println("TABLE NAME: " + name);
        String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
        String[] els = fields.split(",");
        ArrayList<String> names = new ArrayList<String>();
        ArrayList<Type> types = new ArrayList<Type>();
        String primaryKey = "";
        for (String e : els) {
          String[] els2 = e.trim().split(" ");
          names.add(els2[0].trim());
          if (els2[1].trim().toLowerCase().equals("int")) types.add(Type.INT_TYPE);
          else if (els2[1].trim().toLowerCase().equals("string")) types.add(Type.STRING_TYPE);
          else {
            System.out.println("Unknown type " + els2[1]);
            System.exit(0);
          }
          if (els2.length == 3) {
            if (els2[2].trim().equals("pk")) primaryKey = els2[0].trim();
            else {
              System.out.println("Unknown annotation " + els2[2]);
              System.exit(0);
            }
          }
        }
        Type[] typeAr = types.toArray(new Type[0]);
        String[] namesAr = names.toArray(new String[0]);
        TupleDesc t = new TupleDesc(typeAr, namesAr);
        HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
        addTable(tabHf, name, primaryKey);
        System.out.println("Added table : " + name + " with schema " + t);
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(0);
    } catch (IndexOutOfBoundsException e) {
      System.out.println("Invalid catalog entry : " + line);
      System.exit(0);
    }
  }

  class TableInfo {
    private DbFile dbFile;
    private String primaryKey;

    public TableInfo(@NonNull DbFile dbFile, @NonNull String primaryKey) {
      this.dbFile = dbFile;
      this.primaryKey = primaryKey;
    }
  }
}
