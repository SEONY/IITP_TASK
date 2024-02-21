package org.jkiss.dbeaver.ext.goldilocks.model;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.GenericConstants;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericTable;
import org.jkiss.dbeaver.ext.generic.model.GenericTableColumn;
import org.jkiss.dbeaver.ext.generic.model.GenericTableIndexColumn;
import org.jkiss.dbeaver.ext.generic.model.GenericUtils;
import org.jkiss.dbeaver.model.DBIcon;
import org.jkiss.dbeaver.model.DBPImage;
import org.jkiss.dbeaver.model.DBPImageProvider;
import org.jkiss.dbeaver.model.DBPNamedObject2;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCConstants;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCCompositeCache;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.utils.ArrayUtils;
import org.jkiss.utils.CommonUtils;

public class GoldilocksTable extends GenericTable implements DBPImageProvider, DBPNamedObject2{

    private String tableId;
    private String tablesapceName;
    final public IndexCache indexCache = new IndexCache();

    public GoldilocksTable(GenericStructContainer container, String tableId, String tableName, String tablespaceName, String tableType,
            JDBCResultSet dbResult) {
        super(container, tableName, tableType, dbResult);
        this.tableId        = tableId;
        this.tablesapceName = tablespaceName;
    }

    @Property(hidden = true)
    public String getTableType() {
        return super.getTableType();
    }

    @Property(viewable = true, order = 2)
    public String getTablespace() {
        return tablesapceName;
    }

    @Override
    public DBPImage getObjectImage() {
        return DBIcon.TREE_TABLE;
    }
    
    public String getTableId() {
        return tableId;
    }
    
    @Override
    public Collection<GoldilocksIndex> getIndexes(DBRProgressMonitor monitor) throws DBException {
        return indexCache.getObjects(monitor, getContainer(), this);
    }

    @Override
    public synchronized DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        indexCache.clearCache();
        return super.refreshObject(monitor);
    }

    /**
     * Index cache implementation
     */
    class IndexCache extends
            JDBCCompositeCache<GenericStructContainer, GoldilocksTable, GoldilocksIndex, GenericTableIndexColumn> {
        IndexCache() {
            super(getCache(), GoldilocksTable.class,
                    GenericUtils.getColumn(getCache().getDataSource(), GenericConstants.OBJECT_INDEX,
                            JDBCConstants.TABLE_NAME),
                    GenericUtils.getColumn(getCache().getDataSource(), GenericConstants.OBJECT_INDEX,
                            JDBCConstants.INDEX_NAME));
        }

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(JDBCSession session, GenericStructContainer owner,
                GoldilocksTable forParent) throws SQLException {
            JDBCPreparedStatement dbStat;
            dbStat = session.prepareStatement("SELECT "
                    + "     idx.INDEX_ID        AS INDEX_ID "
                    + "   , idx.INDEX_NAME      AS INDEX_NAME "
                    + "   , spc.TABLESPACE_NAME AS TABLESPACE_NAME "
                    + "   , idx.INDEX_TYPE      AS INDEX_TYPE "
                    + "   , idx.IS_UNIQUE       AS IS_UNIQUE "
                    + "   , CAST( ( SELECT stat.NUM_DISTINCT "
                    + "               FROM DEFINITION_SCHEMA.STAT_INDEX@LOCAL AS stat "
                    + "              WHERE stat.INDEX_ID = idx.INDEX_ID ) "
                    + "            AS NUMBER )  AS CARDINALITY "
                    + "   , idx.COMMENTS        AS COMMENTS "
                    + "FROM " 
                    + "   ( DEFINITION_SCHEMA.INDEXES@LOCAL     AS idx "
                    + "     LEFT OUTER JOIN "
                    + "     DEFINITION_SCHEMA.TABLESPACES@LOCAL AS spc "
                    + "     ON idx.TABLESPACE_ID = spc.TABLESPACE_ID ) "
                    + "   , DEFINITION_SCHEMA.SCHEMATA@LOCAL    AS sch "
                    + "WHERE "
                    + "     idx.SCHEMA_ID   = sch.SCHEMA_ID " 
                    + " AND sch.SCHEMA_NAME = ? "
                    + (forParent != null ? " AND idx.TABLE_ID    = ?" : ""));      
            dbStat.setString(1, owner.getSchema().getName());
            if (forParent != null) {
                dbStat.setString(2, forParent.getTableId());
            }
            return dbStat;
        }

        @Nullable
        @Override
        protected GoldilocksIndex fetchObject(JDBCSession session, GenericStructContainer owner, GoldilocksTable parent,
                String indexName, JDBCResultSet dbResult) {
            String tablespace = JDBCUtils.safeGetString(dbResult, "TABLESPACE_NAME").trim();
            String goldilocksIndexName = CommonUtils.notEmpty(JDBCUtils.safeGetString(dbResult, "INDEX_NAME")).trim();
            String comment = JDBCUtils.safeGetString(dbResult, "COMMENTS");
            String indexType = CommonUtils.notEmpty(JDBCUtils.safeGetString(dbResult, "INDEX_TYPE")).trim();
            boolean isNonUnique = !JDBCUtils.safeGetBoolean(dbResult, "IS_UNIQUE");
            long cardinality = JDBCUtils.safeGetLong(dbResult, "CARDINALITY");

            return new GoldilocksIndex(parent, isNonUnique, owner.getSchema().getName(), cardinality,
                    goldilocksIndexName, true, tablespace, indexType, comment );
        }

        @Nullable
        @Override
        protected GenericTableIndexColumn[] fetchObjectRow(JDBCSession session, GoldilocksTable parent,
                GoldilocksIndex index, JDBCResultSet dbResult) throws DBException
        {
            ArrayList<GenericTableIndexColumn> indexColumns = new ArrayList<>();
            
            String indexId = JDBCUtils.safeGetString(dbResult, "INDEX_ID");            
            
            try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT " 
                    + "       col.COLUMN_NAME         AS COLUMN_NAME "
                    + "     , idxc.ORDINAL_POSITION   AS ORDINAL_POSITION "
                    + "     , idxc.IS_ASCENDING_ORDER AS IS_ASCENDING_ORDER "
                    + "  FROM DEFINITION_SCHEMA.INDEX_KEY_COLUMN_USAGE@LOCAL AS idxc "
                    + "     , DEFINITION_SCHEMA.COLUMNS@LOCAL                AS col "
                    + " WHERE "
                    + "       idxc.COLUMN_ID = col.COLUMN_ID "
                    + "   AND idxc.INDEX_ID = ?")) {
                dbStat.setString(1, indexId);
                String columnName = "";
                int ordinalPosition = 0;
                boolean ascending = false;
                try (JDBCResultSet dbResult2 = dbStat.executeQuery()) {
                    while (dbResult2.nextRow()) {
                        columnName = dbResult2.getString(1);
                        ordinalPosition = dbResult2.getInt(2);
                        ascending = dbResult2.getBoolean(3);
                        
                        GenericTableColumn tableColumn = parent.getAttribute(session.getProgressMonitor(), columnName);
                        if (tableColumn == null) {
                            log.debug("Column '" + columnName + "' not found in table '" + parent.getName()
                                    + "' for index '" + index.getName() + "'");
                            return null;
                        }
                        indexColumns.add(new GenericTableIndexColumn(index, tableColumn, ordinalPosition, ascending));
                    }
                }
            } catch (SQLException e) {
                throw new DBException(e, session.getDataSource());
            }

            return ArrayUtils.toArray(GenericTableIndexColumn.class, indexColumns);
        }

        @Override
        protected void cacheChildren(DBRProgressMonitor monitor, GoldilocksIndex index,
                List<GenericTableIndexColumn> rows) {
            index.setColumns(rows);
        }
    }
}
