package org.jkiss.dbeaver.ext.goldilocks.model;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.generic.GenericConstants;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.generic.model.GenericFunctionResultType;
import org.jkiss.dbeaver.ext.generic.model.GenericObjectContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericProcedure;
import org.jkiss.dbeaver.ext.generic.model.GenericSequence;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericSynonym;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.GenericTableColumn;
import org.jkiss.dbeaver.ext.generic.model.GenericTableConstraintColumn;
import org.jkiss.dbeaver.ext.generic.model.GenericTableIndex;
import org.jkiss.dbeaver.ext.generic.model.GenericUniqueKey;
import org.jkiss.dbeaver.ext.generic.model.GenericUtils;
import org.jkiss.dbeaver.ext.generic.model.GenericView;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaModel;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaObject;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCConstants;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSEntityConstraintType;
import org.jkiss.dbeaver.model.struct.rdb.DBSIndexType;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;
import org.jkiss.utils.ArrayUtils;
import org.jkiss.utils.CommonUtils;

public class GoldilocksMetaModel extends GenericMetaModel {
    
    private static final Log log = Log.getLog(GoldilocksMetaModel.class);

    public GoldilocksMetaModel() {
        super();
    }
    
    private class PrivItem {
        long grantorID;
        String grantorName;
        long granteeID;
        String granteeName;
        boolean isBuiltIn;
        boolean isGrantable;
        int privOrder;

        PrivItem(long grantorID, String grantorName, long granteeID, String granteeName, boolean isBuiltIn,
                boolean isGrantable) {
            this.grantorID = grantorID;
            this.grantorName = grantorName;
            this.granteeID = granteeID;
            this.granteeName = granteeName;
            this.isBuiltIn = isBuiltIn;
            this.isGrantable = isGrantable;
            this.privOrder = -1;
        }
    }

    //////////////////////////////////////////////////////
    // Datasource

    public GenericDataSource createDataSourceImpl(DBRProgressMonitor monitor, DBPDataSourceContainer container)
            throws DBException {
        return new GoldilocksDataSource(monitor, container, this);
    }
    
    //////////////////////////////////////////////////////
    // Misc
    
    @Override
    public String getAutoIncrementClause(GenericTableColumn column) {
        return "GENERATED ALWAYS AS IDENTITY";
    }

    //////////////////////////////////////////////////////
    // Procedure load

    @Override
    public void loadProcedures(DBRProgressMonitor monitor, @NotNull GenericObjectContainer container)
            throws DBException {
        Map<String, GoldilocksPackage> packageMap = null;

        GenericDataSource dataSource = container.getDataSource();
        GenericMetaObject procObject = dataSource.getMetaObject(GenericConstants.OBJECT_PROCEDURE);
        try (JDBCSession session = DBUtils.openMetaSession(monitor, container, "Load procedures")) {
            // Read procedures
            JDBCResultSet dbResult = session.getMetaData().getProcedures(
                    container.getCatalog() == null ? null : container.getCatalog().getName(),
                    container.getSchema() == null || DBUtils.isVirtualObject(container.getSchema()) ? null
                            : JDBCUtils.escapeWildCards(session, container.getSchema().getName()),
                    dataSource.getAllObjectsPattern());
            try {
                while (dbResult.next()) {
                    if (monitor.isCanceled()) {
                        break;
                    }
                    String procedureName = GenericUtils.safeGetStringTrimmed(procObject, dbResult,
                            JDBCConstants.PROCEDURE_NAME);
                    String specificName = GenericUtils.safeGetStringTrimmed(procObject, dbResult,
                            JDBCConstants.SPECIFIC_NAME);
                    int procTypeNum = GenericUtils.safeGetInt(procObject, dbResult, JDBCConstants.PROCEDURE_TYPE);
                    String remarks = GenericUtils.safeGetString(procObject, dbResult, JDBCConstants.REMARKS);
                    DBSProcedureType procedureType;
                    switch (procTypeNum) {
                    case DatabaseMetaData.procedureNoResult:
                        procedureType = DBSProcedureType.PROCEDURE;
                        break;
                    case DatabaseMetaData.procedureReturnsResult:
                        procedureType = DBSProcedureType.FUNCTION;
                        break;
                    case DatabaseMetaData.procedureResultUnknown:
                        procedureType = DBSProcedureType.PROCEDURE;
                        break;
                    default:
                        procedureType = DBSProcedureType.UNKNOWN;
                        break;
                    }
                    if (CommonUtils.isEmpty(specificName)) {
                        specificName = procedureName;
                    }
                    procedureName = GenericUtils.normalizeProcedureName(procedureName);

                    String packageName = "";
                    GoldilocksPackage procedurePackage = null;

                    if (remarks != null && remarks.contains("Packaged")) {
                        packageName = procedureName.substring(0, procedureName.indexOf('.'));
                        procedureName = procedureName.substring(procedureName.indexOf('.') + 1);
                        specificName = procedureName;
                    }

                    if (packageName != null) {
                        if (!CommonUtils.isEmpty(packageName)) {
                            if (packageMap == null) {
                                packageMap = new TreeMap<>();
                            }
                            procedurePackage = packageMap.get(packageName);
                            if (procedurePackage == null) {
                                procedurePackage = new GoldilocksPackage(container, packageName, true);
                                packageMap.put(packageName, procedurePackage);
                                container.addPackage(procedurePackage);
                            }
                        }
                    }

                    final GenericProcedure procedure = createProcedureImpl(
                            procedurePackage != null ? procedurePackage : container, procedureName, specificName,
                            remarks, procedureType, null);
                    if (procedurePackage != null) {
                        procedurePackage.addProcedure(procedure);
                    } else {
                        container.addProcedure(procedure);
                    }
                }
            } finally {
                dbResult.close();
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
    }

    @Override
    public GenericProcedure createProcedureImpl(GenericStructContainer container, String procedureName,
            String specificName, String remarks, DBSProcedureType procedureType,
            GenericFunctionResultType functionResultType) {
        return new GoldilocksProcedure(container, procedureName, specificName, remarks, procedureType,
                functionResultType);
    }
    
    private String getProcedureId(GenericDataSource dataSource, JDBCSession session, String schemaName, String procedureName)
            throws DBException {
        String procedureId = "";
        
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "       rtn.SPECIFIC_ID "
                        + "  FROM "
                        + "       DEFINITION_SCHEMA.ROUTINES@LOCAL rtn "
                        + "     , DEFINITION_SCHEMA.SCHEMATA@LOCAL sch "
                        + " WHERE "
                        + "       rtn.SPECIFIC_SCHEMA_ID = sch.SCHEMA_ID "
                        + "   AND rtn.SPECIFIC_NAME = ? "
                        + "   AND sch.SCHEMA_NAME = ? "
                        + "   AND rtn.MODULE_ID IS NULL ")) {
            dbStat.setString(1, procedureName);
            dbStat.setString(2, schemaName);
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    procedureId = dbResult.getString(1);
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
        return procedureId;
    }
    
    private String getCreateProcedure(GenericDataSource dataSource, JDBCSession session, String procedureId)
            throws DBException {
        String procedureString = "";
        try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                + "        auth.AUTHORIZATION_NAME  "
                + "      , sch.SCHEMA_NAME          "
                + "      , rtn.SPECIFIC_NAME        "
                + "      , rtn.ROUTINE_DEFINITION   "
                + "   FROM "
                + "        DEFINITION_SCHEMA.ROUTINES@LOCAL              AS rtn "
                + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL              AS sch  "
                + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL        AS auth "
                + "  WHERE "
                + "        rtn.SPECIFIC_SCHEMA_ID   = sch.SCHEMA_ID "
                + "    AND rtn.SPECIFIC_OWNER_ID    = auth.AUTH_ID "
                + "    AND rtn.SPECIFIC_ID = ? "
                + "    AND rtn.MODULE_ID IS NULL ")) {
            dbStat.setString(1, procedureId);

            String ownerName = "";
            String definition = "";

            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    ownerName = dbResult.getString(1);
                    definition = dbResult.getString(4).trim();
                }
            }
            
            procedureString = procedureString
                    + "\nSET SESSION AUTHORIZATION \"" + ownerName + "\"; "
                    + "\nCREATE OR REPLACE " + definition
                    + "\n/\n"
                    +"COMMIT;\n";
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }

        return procedureString;
    }

    private String getGrantProcedure(GenericDataSource dataSource, JDBCSession session, String procedureId)
            throws DBException {        
        String GrantProcedureDDL = "";
        String isBuiltIn = "NO";
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "        rtn.SPECIFIC_OWNER_ID "
                        + "      , grantor.AUTH_ID "
                        + "      , grantor.AUTHORIZATION_NAME  "
                        + "      , grantee.AUTH_ID "
                        + "      , grantee.AUTHORIZATION_NAME  "
                        + "      , sch.SCHEMA_NAME          "
                        + "      , rtn.SPECIFIC_NAME        "
                        + "      , CAST( CASE WHEN rpriv.IS_GRANTABLE = TRUE THEN 'YES' "
                        + "                                                  ELSE 'NO'  "
                        + "                   END  "
                        + "              AS VARCHAR(32 OCTETS) ) "
                        + "   FROM "
                        + "        DEFINITION_SCHEMA.ROUTINES@LOCAL              AS rtn "
                        + "      , DEFINITION_SCHEMA.ROUTINE_PRIVILEGES@LOCAL    AS rpriv "
                        + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL              AS sch  "
                        + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL        AS grantor "
                        + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL        AS grantee "
                        + "  WHERE "
                        + "        grantor.AUTHORIZATION_NAME <> '_SYSTEM' "
                        + "    AND grantee.AUTH_ID >= ( SELECT AUTH_ID "
                        + "                               FROM DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL "
                        + "                              WHERE AUTHORIZATION_NAME = 'PUBLIC' ) "
                        + "    AND rtn.SPECIFIC_ID   = rpriv.SPECIFIC_ID "
                        + "    AND rtn.SPECIFIC_SCHEMA_ID     = sch.SCHEMA_ID "
                        + "    AND rpriv.GRANTOR_ID  = grantor.AUTH_ID "
                        + "    AND rpriv.GRANTEE_ID  = grantee.AUTH_ID "
                        + "    AND rtn.SPECIFIC_ID = ? "
                        + "    AND rtn.MODULE_ID IS NULL "
                        + "  ORDER BY "
                        + "        grantor.AUTH_ID "
                        + "      , grantee.AUTH_ID ")) {
            dbStat.setString(1, procedureId);

            long ownerId = 0;
            long grantorID = 0;
            String grantorName = "";
            long granteeID = 0;
            String granteeName = "";
            String schemaName = "";
            String procName = "";
            String grantable = "";

            LinkedList<PrivItem> privItemList = new LinkedList<PrivItem>();

            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                while (dbResult.nextRow()) {
                    ownerId = dbResult.getLong(1);
                    grantorID = dbResult.getLong(2);
                    grantorName = dbResult.getString(3);
                    granteeID = dbResult.getLong(4);
                    granteeName = dbResult.getString(5);
                    schemaName = dbResult.getString(6);
                    procName = dbResult.getString(7);
                    grantable = dbResult.getString(8);

                    privItemList.add(new PrivItem(grantorID, grantorName, granteeID, granteeName,
                            isBuiltIn.contains("YES") ? true : false, grantable.contains("YES") ? true : false));
                }

                LinkedList<PrivItem> privOrderList = buildPrivOrder(privItemList, ownerId);
                GrantProcedureDDL = GrantProcedureDDL
                        + getGrantByPrivOrder(privOrderList, "PROCEDURE", null, schemaName, procName, null);
            }

            return GrantProcedureDDL;
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
    }
    
    private String getCommentProcedure(GenericDataSource dataSource, JDBCSession session, String procedureId)
            throws DBException {
        String commentDDL = "";        
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "        auth.AUTHORIZATION_NAME  "
                        + "      , sch.SCHEMA_NAME          "
                        + "      , rtn.SPECIFIC_NAME        "
                        + "      , rtn.COMMENTS             "
                        + "   FROM "
                        + "        DEFINITION_SCHEMA.ROUTINES@LOCAL              AS rtn "
                        + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL              AS sch  "
                        + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL        AS auth "
                        + "  WHERE "
                        + "        rtn.SPECIFIC_SCHEMA_ID   = sch.SCHEMA_ID "
                        + "    AND rtn.SPECIFIC_OWNER_ID    = auth.AUTH_ID "
                        + "    AND rtn.SPECIFIC_ID = ? "
                        + "    AND rtn.MODULE_ID IS NULL ")) {
            dbStat.setString(1, procedureId);

            String ownerName = "";
            String schemaName = "";
            String procName = "";
            String comment = "";
            boolean nullComment = false;
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    ownerName = dbResult.getString(1);
                    schemaName = dbResult.getString(2);
                    procName = dbResult.getString(3);
                    comment = dbResult.getString(4);
                    nullComment = dbResult.wasNull();

                    if (nullComment == false) {
                        commentDDL = commentDDL
                                + "\nSET SESSION AUTHORIZATION \"" + ownerName + "\"; "
                                + "\nCOMMENT "
                                + "\n    ON PROCEDURE \"" + schemaName + "\".\"" + procName + "\" "
                                + "\n    IS '" + comment + "' "
                                + "\n;\n"
                                + "COMMIT;\n";
                    }
                }
            }
            
            return commentDDL;
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
    }

    private String getPackageId(GenericDataSource dataSource, JDBCSession session, String schemaName,
            String packageName) throws DBException {
        String packageId = "";
        
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "       mdl.MODULE_ID "
                        + "  FROM "
                        + "       DEFINITION_SCHEMA.MODULES@LOCAL  mdl "
                        + "     , DEFINITION_SCHEMA.SCHEMATA@LOCAL sch "
                        + " WHERE "
                        + "       mdl.MODULE_SCHEMA_ID = sch.SCHEMA_ID "
                        + "   AND mdl.MODULE_NAME = ? "
                        + "   AND sch.SCHEMA_NAME = ? ")) {
            dbStat.setString(1, packageName);
            dbStat.setString(2, schemaName);
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    packageId = dbResult.getString(1);
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
        return packageId;
    }
    
    private String getCreatePackage(GenericDataSource dataSource, JDBCSession session, String packageId)
            throws DBException {
        String packageString = "";
        try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                + "        auth.AUTHORIZATION_NAME  "
                + "      , sch.SCHEMA_NAME          "
                + "      , mdl.MODULE_NAME          "
                + "      , mdl.MODULE_DEFINITION    "
                + "      , mdlbd.MODULE_DEFINITION  "
                + "   FROM "
                + "        DEFINITION_SCHEMA.MODULES@LOCAL                      AS mdl   "
                + "        LEFT OUTER JOIN DEFINITION_SCHEMA.MODULE_BODY@LOCAL  AS mdlbd "
                + "            ON mdl.MODULE_ID = mdlbd.MODULE_ID                        "
                + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL                     AS sch   "
                + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL               AS auth  "
                + "  WHERE "
                + "        mdl.MODULE_SCHEMA_ID   = sch.SCHEMA_ID "
                + "    AND mdl.MODULE_OWNER_ID    = auth.AUTH_ID "
                + "    AND mdl.MODULE_ID = ? ")) {
            dbStat.setString(1, packageId);

            String ownerName = "";
            String definition = "";
            String bodyDefinition = "";
            boolean nullBodyDefinition = false;

            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    ownerName = dbResult.getString(1);
                    definition = dbResult.getString(4).trim();
                    bodyDefinition = dbResult.getString(5).trim();
                    nullBodyDefinition = dbResult.wasNull();
                }
            }
            
            packageString = packageString
                    + "\nSET SESSION AUTHORIZATION \"" + ownerName + "\"; "
                    + "\nCREATE OR REPLACE " + definition;
            
            if (nullBodyDefinition == false) {
                packageString = packageString
                        + "\nCREATE OR REPLACE " + bodyDefinition;
            }
            
            packageString = packageString
                    + "\n/\n"
                    +"COMMIT;\n";

            return packageString;
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
    }

    private String getGrantPackage(GenericDataSource dataSource, JDBCSession session, String packageId)
            throws DBException {
        String GrantPackageDDL = "";
        String isBuiltIn = "NO";
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "        mdl.MODULE_OWNER_ID "
                        + "      , grantor.AUTH_ID "
                        + "      , grantor.AUTHORIZATION_NAME  "
                        + "      , grantee.AUTH_ID "
                        + "      , grantee.AUTHORIZATION_NAME  "
                        + "      , sch.SCHEMA_NAME          "
                        + "      , mdl.MODULE_NAME        "
                        + "      , CAST( CASE WHEN mpriv.IS_GRANTABLE = TRUE THEN 'YES' "
                        + "                                                  ELSE 'NO'  "
                        + "                   END  "
                        + "              AS VARCHAR(32 OCTETS) ) "
                        + "   FROM "
                        + "        DEFINITION_SCHEMA.MODULES@LOCAL               AS mdl "
                        + "      , DEFINITION_SCHEMA.MODULE_PRIVILEGES@LOCAL     AS mpriv "
                        + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL              AS sch  "
                        + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL        AS grantor "
                        + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL        AS grantee "
                        + "  WHERE "
                        + "        grantor.AUTHORIZATION_NAME <> '_SYSTEM' "
                        + "    AND grantee.AUTH_ID >= ( SELECT AUTH_ID "
                        + "                               FROM DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL "
                        + "                              WHERE AUTHORIZATION_NAME = 'PUBLIC' ) "
                        + "    AND mdl.MODULE_ID   = mpriv.MODULE_ID "
                        + "    AND mdl.MODULE_SCHEMA_ID     = sch.SCHEMA_ID "
                        + "    AND mpriv.GRANTOR_ID  = grantor.AUTH_ID "
                        + "    AND mpriv.GRANTEE_ID  = grantee.AUTH_ID "
                        + "    AND mdl.MODULE_ID = ? "
                        + "  ORDER BY "
                        + "        grantor.AUTH_ID "
                        + "      , grantee.AUTH_ID ")) {
            dbStat.setString(1, packageId);

            long ownerId = 0;
            long grantorID = 0;
            String grantorName = "";
            long granteeID = 0;
            String granteeName = "";
            String schemaName = "";
            String packageName = "";
            String grantable = "";

            LinkedList<PrivItem> privItemList = new LinkedList<PrivItem>();

            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                while (dbResult.nextRow()) {
                    ownerId = dbResult.getLong(1);
                    grantorID = dbResult.getLong(2);
                    grantorName = dbResult.getString(3);
                    granteeID = dbResult.getLong(4);
                    granteeName = dbResult.getString(5);
                    schemaName = dbResult.getString(6);
                    packageName = dbResult.getString(7);
                    grantable = dbResult.getString(8);

                    privItemList.add(new PrivItem(grantorID, grantorName, granteeID, granteeName,
                            isBuiltIn.contains("YES") ? true : false, grantable.contains("YES") ? true : false));
                }

                LinkedList<PrivItem> privOrderList = buildPrivOrder(privItemList, ownerId);
                GrantPackageDDL = GrantPackageDDL
                        + getGrantByPrivOrder(privOrderList, "PACKAGE", null, schemaName, packageName, null);
            }

            return GrantPackageDDL;
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
    }
    
    private String getCommentPackage(GenericDataSource dataSource, JDBCSession session, String packageId)
            throws DBException {
        String commentDDL = "";
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "        auth.AUTHORIZATION_NAME  "
                        + "      , sch.SCHEMA_NAME          "
                        + "      , mdl.MODULE_NAME          "
                        + "      , mdl.COMMENTS             "
                        + "      , mdlbd.COMMENTS             "
                        + "   FROM "
                        + "        DEFINITION_SCHEMA.MODULES@LOCAL                      AS mdl "
                        + "        LEFT OUTER JOIN DEFINITION_SCHEMA.MODULE_BODY@LOCAL  AS mdlbd "
                        + "            ON mdl.MODULE_ID = mdlbd.MODULE_ID                        "
                        + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL                     AS sch  "
                        + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL               AS auth "
                        + "  WHERE "
                        + "        mdl.MODULE_SCHEMA_ID   = sch.SCHEMA_ID "
                        + "    AND mdl.MODULE_OWNER_ID    = auth.AUTH_ID "
                        + "    AND mdl.MODULE_ID = ? ")) {
            dbStat.setString(1, packageId);

            String ownerName = "";
            String schemaName = "";
            String packageName = "";
            String comment = "";
            boolean nullComment = false;
            String bodyComment = "";
            boolean nullBodyComment = false;
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    ownerName = dbResult.getString(1);
                    schemaName = dbResult.getString(2);
                    packageName = dbResult.getString(3);
                    comment = dbResult.getString(4);
                    nullComment = dbResult.wasNull();
                    bodyComment = dbResult.getString(5);
                    nullBodyComment = dbResult.wasNull();

                    if ((nullComment == false) || (nullBodyComment == false)) {
                        commentDDL = commentDDL
                                + "\nSET SESSION AUTHORIZATION \"" + ownerName + "\"; ";
                        
                        if (nullComment == false) {
                            commentDDL = commentDDL
                                    + "\nCOMMENT "
                                    + "\n    ON PACKAGE \"" + schemaName + "\".\"" + packageName + "\" "
                                    + "\n    IS '" + comment + "' ";
                        }
                        
                        if (nullBodyComment == false) {
                            commentDDL = commentDDL
                                    + "\nCOMMENT "
                                    + "\n    ON PACKAGE BODY \"" + schemaName + "\".\"" + packageName + "\" "
                                    + "\n    IS '" + bodyComment + "' ";
                        }
                        
                        commentDDL = commentDDL
                                + "\n;\n"
                                + "COMMIT;\n";
                    }
                }
            }
            
            return commentDDL;
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
    }

    @Override
    public String getProcedureDDL(DBRProgressMonitor monitor, GenericProcedure sourceObject) throws DBException {
        GenericDataSource dataSource = sourceObject.getDataSource();

        try (JDBCSession session = DBUtils.openMetaSession(monitor, sourceObject, "Read Goldilocks procedure source")) {
            if (sourceObject.getPackage() == null) {
                String procedureId = getProcedureId(dataSource, session, sourceObject.getSchema().getName(),
                        sourceObject.getName());

                return getCreateProcedure(dataSource, session, procedureId)
                        + getGrantProcedure(dataSource, session, procedureId)
                        + getCommentProcedure(dataSource, session, procedureId);
            } else {
                String packageId = getPackageId(dataSource, session, sourceObject.getSchema().getName(),
                        sourceObject.getPackage().getName());

                return getCreatePackage(dataSource, session, packageId)
                        + getGrantPackage(dataSource, session, packageId)
                        + getCommentPackage(dataSource, session, packageId);
            }
        }
    }

    //////////////////////////////////////////////////////
    // Packages

    public String getPackageDDL(DBRProgressMonitor monitor, GoldilocksPackage sourceObject) throws DBException {
        GenericDataSource dataSource = sourceObject.getDataSource();

        try (JDBCSession session = DBUtils.openMetaSession(monitor, sourceObject, "Read Goldilocks package source")) {
            String packageId = getPackageId(dataSource, session, sourceObject.getSchema().getName(),
                    sourceObject.getName());

            return getCreatePackage(dataSource, session, packageId) + getGrantPackage(dataSource, session, packageId)
                    + getCommentPackage(dataSource, session, packageId);
        }
    }

    //////////////////////////////////////////////////////
    // Tables
    
    @Override
    public JDBCStatement prepareTableLoadStatement(@NotNull JDBCSession session, @NotNull GenericStructContainer owner,
            @Nullable GenericTableBase object, @Nullable String objectName) throws SQLException {
        JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "       CAST( CURRENT_CATALOG AS VARCHAR(128 OCTETS) ) AS TABLE_CAT "
                        + "     , sch.SCHEMA_NAME AS TABLE_SCHEM "
                        + "     , tab.TABLE_NAME  AS TABLE_NAME "
                        + "     , tab.TABLE_ID    AS TABLE_ID "
                        + "     , spc.TABLESPACE_NAME AS TABLESPACE_NAME "
                        + "     , CAST( CASE WHEN sch.SCHEMA_NAME = 'INFORMATION_SCHEMA' AND tab.TABLE_TYPE = 'VIEW' "
                        + "                  THEN 'SYSTEM TABLE' "
                        + "                  ELSE DECODE( tab.TABLE_TYPE, 'BASE TABLE', 'TABLE', tab.TABLE_TYPE ) "
                        + "                  END  "
                        + "             AS VARCHAR(32 OCTETS) ) AS TABLE_TYPE "
                        + "     , tab.COMMENTS AS REMARKS "
                        + "  FROM   "
                        + "      ( DEFINITION_SCHEMA.TABLES@LOCAL      AS tab " 
                        + "        LEFT OUTER JOIN "
                        + "        DEFINITION_SCHEMA.TABLESPACES@LOCAL AS spc " 
                        + "        ON tab.TABLESPACE_ID = spc.TABLESPACE_ID ) "
                        + "     , DEFINITION_SCHEMA.SCHEMATA@LOCAL    AS sch "
                        + " WHERE "
                        + "       tab.SCHEMA_ID = sch.SCHEMA_ID  "
                        + "   AND tab.IS_DROPPED = FALSE "
                        + "   AND sch.SCHEMA_NAME = ?  "
                        + (object != null ? "   AND tab.TABLE_NAME = ?  " : "")
                        + "   AND EXISTS ( "
                        + "                  SELECT /*+ INDEX( pvtab, TABLE_PRIVILEGES_INDEX_TABLE_ID ) */ "
                        + "                         1 "
                        + "                    FROM DEFINITION_SCHEMA.TABLE_PRIVILEGES@LOCAL AS pvtab "
                        + "                   WHERE pvtab.TABLE_ID = tab.TABLE_ID "
                        + "                     AND pvtab.GRANTEE_ID IN ( USER_ID(), 5 ) "
                        + "                  UNION ALL "
                        + "                  SELECT /*+ INDEX( pvcol, COLUMN_PRIVILEGES_INDEX_TABLE_ID ) */ "
                        + "                         1 "
                        + "                    FROM DEFINITION_SCHEMA.COLUMN_PRIVILEGES@LOCAL AS pvcol  "
                        + "                   WHERE pvcol.TABLE_ID = tab.TABLE_ID "
                        + "                     AND pvcol.GRANTEE_ID IN ( USER_ID(), 5 ) "
                        + "                  UNION ALL "
                        + "                  SELECT /*+ INDEX( pvsch, SCHEMA_PRIVILEGES_INDEX_SCHEMA_ID ) */ "
                        + "                         1 "
                        + "                    FROM DEFINITION_SCHEMA.SCHEMA_PRIVILEGES@LOCAL AS pvsch "
                        + "                   WHERE pvsch.SCHEMA_ID = tab.SCHEMA_ID "
                        + "                     AND pvsch.GRANTEE_ID IN ( USER_ID(), 5 ) "
                        + "                     AND pvsch.PRIVILEGE_TYPE IN ( 'CONTROL SCHEMA', 'ALTER TABLE', 'DROP TABLE',  "
                        + "                                                   'SELECT TABLE', 'INSERT TABLE', 'DELETE TABLE', 'UPDATE TABLE', 'LOCK TABLE' )  "
                        + "                  UNION ALL "
                        + "                  SELECT /*+ INDEX( pvdba, DATABASE_PRIVILEGES_INDEX_GRANTEE_ID ) */ "
                        + "                         1 "
                        + "                    FROM DEFINITION_SCHEMA.DATABASE_PRIVILEGES@LOCAL AS pvdba "
                        + "                   WHERE pvdba.GRANTEE_ID IN ( USER_ID(), 5 ) "
                        + "                     AND pvdba.PRIVILEGE_TYPE IN ( 'ALTER ANY TABLE', 'DROP ANY TABLE',  "
                        + "                                                   'SELECT ANY TABLE', 'INSERT ANY TABLE', 'DELETE ANY TABLE', 'UPDATE ANY TABLE', 'LOCK ANY TABLE' )  "
                        + "              )"
                        + "ORDER BY " 
                        + "      tab.TABLE_NAME ");
        dbStat.setString(1, owner.getSchema().getName());
        if (object != null) {
            dbStat.setString(2, object.getName());
        }
        return dbStat;
    }

    @Override
    public GenericTableBase createTableImpl(@NotNull JDBCSession session, @NotNull GenericStructContainer owner,
            @NotNull GenericMetaObject tableObject, @NotNull JDBCResultSet dbResult) {
        String tableId = GenericUtils.safeGetStringTrimmed(tableObject, dbResult, "TABLE_ID");
        String tableName = GenericUtils.safeGetStringTrimmed(tableObject, dbResult, JDBCConstants.TABLE_NAME);
        String tablespaceName = GenericUtils.safeGetStringTrimmed(tableObject, dbResult, "TABLESPACE_NAME");        
        String tableType = GenericUtils.safeGetStringTrimmed(tableObject, dbResult, JDBCConstants.TABLE_TYPE);
        String tableSchema = GenericUtils.safeGetStringTrimmed(tableObject, dbResult, JDBCConstants.TABLE_SCHEM);
        if (!CommonUtils.isEmpty(tableSchema) && owner.getDataSource().isOmitSchema()) {
            log.debug("Ignore table " + tableSchema + "." + tableName + " (schemas are omitted)");
            return null;
        }

        if (CommonUtils.isEmpty(tableName)) {
            log.debug("Empty table name " + (owner == null ? "" : " in container " + owner.getName()));
            return null;
        }

        if (DBUtils.isVirtualObject(owner) && !CommonUtils.isEmpty(tableSchema)) {
            return null;
        }
        
        GenericTableBase table = createTableImpl(owner, tableId, tableName, tablespaceName, tableType, dbResult);
        if (table == null) {
            return null;
        }

        boolean isSystemTable = table.isSystem();
        if (isSystemTable && !owner.getDataSource().getContainer().getNavigatorSettings().isShowSystemObjects()) {
            return null;
        }
        return table;
    }

    private GenericTableBase createTableImpl(GenericStructContainer container, @Nullable String tableId, @Nullable String tableName,
            @Nullable String tablespaceName, @Nullable String tableType, @Nullable JDBCResultSet dbResult) {
        if (tableType != null && isView(tableType)) {
            return new GoldilocksView(container, tableName, tableType, dbResult);
        }
        return new GoldilocksTable(container, tableId, tableName, tablespaceName, tableType, dbResult);
    }

    private String getCreateTable(GenericDataSource dataSource, JDBCSession session, String tableId, String tableType)
            throws DBException {
        String tableString = "";

        String ownerName = "";
        String schemaName = "";
        String tableName = "";
        String spaceName = "";
        boolean nullSpaceName = false;
        String shardingStrategy = "";
        boolean nullShardingStrategy = true;
        String cacheTableType = "";
        String pctFree = "";
        String pctUsed = "";
        String iniTrans = "";
        String maxTrans = "";
        String segInit = "";
        String segNext = "";
        String segMin = "";
        String segMax = "";

        if (tableType.contains("GLOBAL TEMPORARY")) {
            try (JDBCPreparedStatement dbStat = session
                    .prepareStatement("SELECT " 
                            + "        auth.AUTHORIZATION_NAME   "
                            + "      , sch.SCHEMA_NAME           "
                            + "      , tab.TABLE_NAME            "
                            + "      , spc.TABLESPACE_NAME       "
                            + "   FROM "
                            + "        DEFINITION_SCHEMA.TABLES@LOCAL           AS tab  "
                            + "        LEFT OUTER JOIN DEFINITION_SCHEMA.TABLESPACES@LOCAL  AS spc "
                            + "            ON tab.TABLESPACE_ID = spc.TABLESPACE_ID "
                            + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL         AS sch  "
                            + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL   AS auth  "
                            + "  WHERE "
                            + "        tab.SCHEMA_ID   = sch.SCHEMA_ID "
                            + "    AND tab.OWNER_ID    = auth.AUTH_ID "
                            + "    AND tab.TABLE_ID    = ? ")) {
                dbStat.setString(1, tableId);
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    if (dbResult.nextRow()) {
                        ownerName = dbResult.getString(1);
                        schemaName = dbResult.getString(2);
                        tableName = dbResult.getString(3);
                        spaceName = dbResult.getString(4);
                        nullSpaceName = dbResult.wasNull();
                    }
                }
                
                tableString = tableString
                        + "\nSET SESSION AUTHORIZATION \"" + ownerName + "\"; "
                        + "CREATE GLOBAL TEMPORARY TABLE ";

            } catch (SQLException e) {
                throw new DBException(e, dataSource);
            }
        } else {
            try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                    + "        auth.AUTHORIZATION_NAME   "
                    + "      , sch.SCHEMA_NAME           "
                    + "      , tab.TABLE_NAME            "
                    + "      , spc.TABLESPACE_NAME       "
                    + "      , CASE WHEN (sch.IS_BUILTIN = FALSE OR sch.SCHEMA_NAME = 'PUBLIC') "
                    + "             THEN tab.SHARDING_STRATEGY  "
                    + "             ELSE NULL "
                    + "        END "
                    + "      , tphy.TABLE_TYPE          "
                    + "      , tphy.PCTFREE             "
                    + "      , tphy.PCTUSED             "
                    + "      , tphy.INITRANS            "
                    + "      , tphy.MAXTRANS            "
                    + "      , ( tphy.INITIAL_EXTENTS * xspc.EXTSIZE ) "
                    + "      , ( tphy.NEXT_EXTENTS * xspc.EXTSIZE )    "
                    + "      , ( tphy.MIN_EXTENTS * xspc.EXTSIZE )     "
                    + "      , ( tphy.MAX_EXTENTS * xspc.EXTSIZE )     "
                    + "   FROM "
                    + "        DEFINITION_SCHEMA.TABLES@LOCAL           AS tab  "
                    + "      , DEFINITION_SCHEMA.TABLESPACES@LOCAL      AS spc   "
                    + "      , FIXED_TABLE_SCHEMA.X$TABLESPACE@LOCAL    AS xspc   "
                    + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL         AS sch  "
                    + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL   AS auth  "
                    + "      , ( SELECT "
                    + "                 ssch.SCHEMA_NAME            AS schema_name "
                    + "               , stab.TABLE_NAME             AS table_name  "
                    + "               , MAX( xtcac.TABLE_TYPE )     AS table_type "
                    + "               , MAX( xtcac.PCTFREE )        AS pctfree "
                    + "               , MAX( xtcac.PCTUSED )        AS pctused "
                    + "               , MAX( xtcac.INITRANS )       AS initrans "
                    + "               , MAX( xtcac.MAXTRANS )       AS maxtrans "
                    + "               , MAX( xseg.INITIAL_EXTENTS ) AS initial_extents "
                    + "               , MAX( xseg.NEXT_EXTENTS )    AS next_extents "
                    + "               , MAX( xseg.MIN_EXTENTS )     AS min_extents "
                    + "               , MAX( xseg.MAX_EXTENTS )     AS max_extents "
                    + "            FROM  "
                    + "                 DEFINITION_SCHEMA.SCHEMATA@GLOBAL[IGNORE_INACTIVE_MEMBER]       AS ssch  "
                    + "               , DEFINITION_SCHEMA.TABLES@GLOBAL[IGNORE_INACTIVE_MEMBER]         AS stab  "
                    + "               , FIXED_TABLE_SCHEMA.X$TABLE_CACHE@GLOBAL[IGNORE_INACTIVE_MEMBER] AS xtcac "
                    + "               , FIXED_TABLE_SCHEMA.X$SEGMENT@GLOBAL[IGNORE_INACTIVE_MEMBER]     AS xseg  "
                    + "           WHERE "
                    + "                 ssch.CLUSTER_MEMBER_ID = stab.CLUSTER_MEMBER_ID "
                    + "             AND ssch.SCHEMA_ID         = stab.SCHEMA_ID "
                    + "             AND stab.CLUSTER_MEMBER_ID = xtcac.CLUSTER_MEMBER_ID "
                    + "             AND stab.PHYSICAL_ID       = xtcac.PHYSICAL_ID  "
                    + "             AND stab.CLUSTER_MEMBER_ID = xseg.CLUSTER_MEMBER_ID "
                    + "             AND stab.PHYSICAL_ID       = xseg.PHYSICAL_ID "
                    + "           GROUP BY "
                    + "                 ssch.SCHEMA_NAME " + "               , stab.TABLE_NAME "
                    + "        ) AS tphy "
                    + "  WHERE "
                    + "        tab.TABLESPACE_ID = spc.TABLESPACE_ID "
                    + "    AND tab.SCHEMA_ID     = sch.SCHEMA_ID "
                    + "    AND tab.OWNER_ID      = auth.AUTH_ID "
                    + "    AND tab.TABLESPACE_ID = xspc.ID "
                    + "    AND sch.SCHEMA_NAME   = tphy.SCHEMA_NAME "
                    + "    AND tab.TABLE_NAME    = tphy.TABLE_NAME "
                    + "    AND tab.TABLE_ID      = ? ")) {
                dbStat.setString(1, tableId);
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    if (dbResult.nextRow()) {
                        ownerName = dbResult.getString(1);
                        schemaName = dbResult.getString(2);
                        tableName = dbResult.getString(3);
                        spaceName = dbResult.getString(4);
                        nullSpaceName = dbResult.wasNull();
                        shardingStrategy = dbResult.getString(5);
                        nullShardingStrategy = dbResult.wasNull();
                        cacheTableType = dbResult.getString(6);
                        pctFree = dbResult.getString(7);
                        pctUsed = dbResult.getString(8);
                        iniTrans = dbResult.getString(9);
                        maxTrans = dbResult.getString(10);
                        segInit = dbResult.getString(11);
                        segNext = dbResult.getString(12);
                        segMin = dbResult.getString(13);
                        segMax = dbResult.getString(14);
                    }
                }
                
                tableString = tableString
                        + "\nSET SESSION AUTHORIZATION \"" + ownerName + "\"; ";
                
                if (tableType.contains("IMMUTABLE TABLE")) {
                    tableString = tableString
                            + "\nCREATE IMMUTABLE TABLE ";
                } else {
                    tableString = tableString
                            + "\nCREATE TABLE ";
                }
            } catch (SQLException e) {
                throw new DBException(e, dataSource);
            }
        }

        tableString = tableString + "\"" + schemaName + "\".\"" + tableName + "\" ";

        try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                + "        col.COLUMN_NAME "
                + "      , dtd.DATA_TYPE_ID "
                + "      , dtd.STRING_LENGTH_UNIT_ID "
                + "      , dtd.CHARACTER_MAXIMUM_LENGTH "
                + "      , dtd.NUMERIC_PRECISION "
                + "      , dtd.NUMERIC_SCALE "
                + "      , dtd.DATETIME_PRECISION "
                + "      , dtd.INTERVAL_TYPE_ID "
                + "      , dtd.INTERVAL_PRECISION "
                + "      , RTRIM( col.COLUMN_DEFAULT ) "
                + "      , CAST( CASE WHEN col.IS_IDENTITY = TRUE "
                + "                   THEN 'YES' "
                + "                   ELSE 'NO' "
                + "                   END "
                + "              AS VARCHAR(3 OCTETS) ) "
                + "      , col.IDENTITY_GENERATION "
                + "      , col.IDENTITY_START "
                + "      , col.IDENTITY_INCREMENT "
                + "      , col.IDENTITY_MAXIMUM "
                + "      , col.IDENTITY_MINIMUM "
                + "      , CASE WHEN col.IDENTITY_CYCLE = TRUE  "
                + "             THEN 'Y' "
                + "             ELSE 'N' "
                + "             END "
                + "      , col.IDENTITY_CACHE_SIZE "
                + "   FROM   "
                + "        DEFINITION_SCHEMA.COLUMNS@LOCAL AS col "
                + "      , DEFINITION_SCHEMA.DATA_TYPE_DESCRIPTOR@LOCAL AS dtd "
                + "      , DEFINITION_SCHEMA.TABLES@LOCAL  AS tab  "
                + "  WHERE  "
                + "        col.DTD_IDENTIFIER = dtd.DTD_IDENTIFIER "
                + "    AND col.TABLE_ID  = tab.TABLE_ID "
                + "    AND col.IS_UNUSED = FALSE "
                + "    AND col.TABLE_ID  = ? "
                + " ORDER BY  "
                + "        col.PHYSICAL_ORDINAL_POSITION ")) {
            dbStat.setString(1, tableId);
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                String columnName = "";
                int typeId = 0;
                int lengthUnit = 0;
                String stringMaxLen = "";
                String numPrec = "";
                int numScale = 0;
                String timePrec = "";
                int intervalType = 0;
                String intervalPrec = "";
                String defaultLong = "";
                boolean nullDefaultLong = false;
                String isIdentity = "";
                String identityGen = "";
                String identityStart = "";
                String identityInc = "";
                String identityMax = "";
                String identityMin = "";
                String isCycle = "";
                int identityCache = 0;

                tableString = tableString
                        + "\n    ( ";

                int colCount = 0;
                while (dbResult.nextRow()) {
                    columnName = dbResult.getString(1);
                    typeId = dbResult.getInt(2);
                    lengthUnit = dbResult.getInt(3);
                    stringMaxLen = dbResult.getString(4);
                    numPrec = dbResult.getString(5);
                    numScale = dbResult.getInt(6);
                    timePrec = dbResult.getString(7);
                    intervalType = dbResult.getInt(8);
                    intervalPrec = dbResult.getString(9);
                    defaultLong = dbResult.getString(10);
                    nullDefaultLong = dbResult.wasNull();
                    isIdentity = dbResult.getString(11);
                    identityGen = dbResult.getString(12);
                    identityStart = dbResult.getString(13);
                    identityInc = dbResult.getString(14);
                    identityMax = dbResult.getString(15);
                    identityMin = dbResult.getString(16);
                    isCycle = dbResult.getString(17);
                    identityCache = dbResult.getInt(18);

                    if (colCount == 0) {
                        tableString = tableString
                                + "\n        \"" + columnName + "\"";
                    } else {
                        tableString = tableString
                                + "\n      , \"" + columnName + "\"";
                    }

                    switch (typeId) {
                    case 0:
                        tableString = tableString + " BOOLEAN";
                        break;
                    case 1:
                        tableString = tableString + " NATIVE_SMALLINT";
                        break;
                    case 2:
                        tableString = tableString + " NATIVE_INTEGER";
                        break;
                    case 3:
                        tableString = tableString + " NATIVE_BIGINT";
                        break;
                    case 4:
                        tableString = tableString + " NATIVE_REAL";
                        break;
                    case 5:
                        tableString = tableString + " NATIVE_DOUBLE";
                        break;
                    case 6:
                        tableString = tableString + " FLOAT( " + numPrec + " )";
                        break;
                    case 7:
                        if (numScale == Integer.MIN_VALUE) {
                            tableString = tableString + " NUMBER";
                        } else {
                            tableString = tableString + " NUMBER( " + numPrec + ", " + numScale + " )";
                        }
                        break;
                    case 8:
                        break;
                    case 9:
                        tableString = tableString + " CHAR( " + stringMaxLen + " "
                                + (lengthUnit == 1 ? "CHARACTERS" : "OCTETS") + " )";
                        break;
                    case 10:
                        tableString = tableString + " VARCHAR( " + stringMaxLen + " "
                                + (lengthUnit == 1 ? "CHARACTERS" : "OCTETS") + " )";
                        break;
                    case 11:
                        tableString = tableString + " LONG VARCHAR";
                        break;
                    case 12:
                        break;
                    case 13:
                        tableString = tableString + " BINARY( " + stringMaxLen + " )";
                        break;
                    case 14:
                        tableString = tableString + " VARBINARY( " + stringMaxLen + " )";
                        break;
                    case 15:
                        tableString = tableString + " LONG VARBINARY";
                        break;
                    case 16:
                        break;
                    case 17:
                        tableString = tableString + " DATE";
                        break;
                    case 18:
                        tableString = tableString + " TIME( " + timePrec + " ) WITHOUT TIME ZONE";
                        break;
                    case 19:
                        tableString = tableString + " TIMESTAMP( " + timePrec + " ) WITHOUT TIME ZONE";
                        break;
                    case 20:
                        tableString = tableString + " TIME( " + timePrec + " ) WITH TIME ZONE";
                        break;
                    case 21:
                        tableString = tableString + " TIMESTAMP( " + timePrec + " ) WITH TIME ZONE";
                        break;
                    case 22:
                    case 23:
                        switch (intervalType) {
                        case 1:
                            tableString = tableString + " INTERVAL YEAR( " + intervalPrec + " )";
                            break;
                        case 2:
                            tableString = tableString + " INTERVAL MONTH( " + intervalPrec + " )";
                            break;
                        case 3:
                            tableString = tableString + " INTERVAL DAY( " + intervalPrec + " )";
                            break;
                        case 4:
                            tableString = tableString + " INTERVAL HOUR( " + intervalPrec + " )";
                            break;
                        case 5:
                            tableString = tableString + " INTERVAL MINUTE( " + intervalPrec + " )";
                            break;
                        case 6:
                            tableString = tableString + " INTERVAL SECOND( " + intervalPrec + ", " + timePrec + " )";
                            break;
                        case 7:
                            tableString = tableString + " INTERVAL YEAR( " + intervalPrec + " ) TO MONTH";
                            break;
                        case 8:
                            tableString = tableString + " INTERVAL DAY( " + intervalPrec + " ) TO HOUR";
                            break;
                        case 9:
                            tableString = tableString + " INTERVAL DAY( " + intervalPrec + " ) TO MINUTE";
                            break;
                        case 10:
                            tableString = tableString + " INTERVAL DAY( " + intervalPrec + " ) TO SECOND( " + timePrec
                                    + " )";
                            break;
                        case 11:
                            tableString = tableString + " INTERVAL HOUR( " + intervalPrec + " ) TO MINUTE";
                            break;
                        case 12:
                            tableString = tableString + " INTERVAL HOUR( " + intervalPrec + " ) TO SECOND( " + timePrec
                                    + " )";
                            break;
                        case 13:
                            tableString = tableString + " INTERVAL MINUTE( " + intervalPrec + " ) TO SECOND( "
                                    + timePrec + " )";
                            break;
                        }
                        break;
                    case 24:
                        tableString = tableString + " ROWID";
                        break;
                    }

                    if (nullDefaultLong == false) {
                        tableString = tableString
                                + "\n            " + defaultLong + " ";
                    }

                    if (isIdentity.compareToIgnoreCase("YES") == 0) {
                        tableString = tableString
                                + "\n            GENERATED " + identityGen + " AS IDENTITY "
                                + "\n            ( "
                                + "\n                START WITH " + identityStart + " "
                                + "\n                INCREMENT BY " + identityInc + " "
                                + "\n                MAXVALUE " + identityMax + " "
                                + "\n                MINVALUE " + identityMin + " ";

                        if (isCycle.compareToIgnoreCase("Y") == 0) {
                            tableString = tableString
                                    + "\n                CYCLE ";
                        } else {
                            tableString = tableString
                                    + "\n                NO CYCLE ";
                        }

                        if (identityCache > 1) {
                            tableString = tableString
                                    + "\n                CACHE " + identityCache + " "
                                    + "\n            ) ";
                        } else {
                            tableString = tableString
                                    + "\n                NO CACHE "
                                    + "\n            ) ";
                        }
                    }
                    colCount++;
                }

                tableString = tableString
                        + "\n    ) ";
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }

        if (tableType.contains("GLOBAL TEMPORARY")) {
            if (nullSpaceName == false) {
                tableString = tableString
                        + "\n    TABLESPACE \"" + spaceName + "\" ";
            }
        } else {
            if (nullShardingStrategy == false) {
                if (shardingStrategy.contains("CLONED")) {
                    try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT DISTINCT "
                            + "        tab.SHARD_PLACEMENT      "
                            + "      , cgrp.GROUP_NAME          "
                            + "      , cgrp.GROUP_ID            "
                            + "   FROM "
                            + "        DEFINITION_SCHEMA.TABLES@LOCAL            AS tab  "
                            + "      , DEFINITION_SCHEMA.TABLE_MEMBER_MAP@LOCAL  AS tmap "
                            + "      , DEFINITION_SCHEMA.CLUSTER_GROUP@LOCAL     AS cgrp "
                            + "  WHERE "
                            + "        tab.TABLE_ID    = tmap.TABLE_ID "
                            + "    AND tmap.GROUP_ID   = cgrp.GROUP_ID "
                            + "    AND tab.TABLE_ID    = ? "
                            + "  ORDER BY "
                            + "        cgrp.GROUP_ID ")) {
                        dbStat.setString(1, tableId);
                        String placement = "";
                        String groupName = "";
                        boolean isFirst = true;
                        try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                            while (dbResult.nextRow()) {
                                placement = dbResult.getString(1);
                                groupName = dbResult.getString(2);

                                if (placement.contains("AT CLUSTER WIDE")) {
                                    tableString = tableString
                                            + "\n    CLONED "
                                            + "\n    AT CLUSTER WIDE ";
                                    break;
                                } else {
                                    if (isFirst == true) {
                                        isFirst = false;
                                        tableString = tableString
                                                + "\n    CLONED "
                                                + "\n    AT CLUSTER GROUP "
                                                + "\n        \"" + groupName + "\" ";
                                    } else {
                                        tableString = tableString
                                                + "\n      , \"" + groupName + "\" ";
                                    }
                                }
                            }
                        }
                    } catch (SQLException e) {
                        throw new DBException(e, dataSource);
                    }
                } else if (shardingStrategy.contains("HASH SHARDING")) {
                    try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                            + "        tab.SHARD_COUNT          "
                            + "      , col.COLUMN_NAME          "
                            + "   FROM "
                            + "        DEFINITION_SCHEMA.TABLES@LOCAL                   AS tab  "
                            + "      , DEFINITION_SCHEMA.SHARD_KEY_COLUMN_USAGE@LOCAL   AS skey  "
                            + "      , DEFINITION_SCHEMA.COLUMNS@LOCAL                  AS col  "
                            + "  WHERE "
                            + "        tab.TABLE_ID    = skey.TABLE_ID "
                            + "    AND skey.COLUMN_ID  = col.COLUMN_ID "
                            + "    AND tab.TABLE_ID    = ? "
                            + "  ORDER BY "
                            + "        skey.ORDINAL_POSITION ")) {
                        dbStat.setString(1, tableId);
                        int shardCount = 0;
                        String columnName = "";
                        boolean isFirst = true;
                        boolean hasData = false;
                        try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                            while (dbResult.nextRow()) {
                                hasData = true;
                                shardCount = dbResult.getInt(1);
                                columnName = dbResult.getString(2);

                                if (isFirst == true) {
                                    isFirst = false;
                                    tableString = tableString
                                            + "\n    SHARDING BY HASH "
                                            + "\n    ( "
                                            + "\n        \"" + columnName + "\" ";
                                } else {
                                    tableString = tableString
                                            + "\n      , \"" + columnName + "\" ";
                                }
                            }
                            if (hasData == true) {
                                tableString = tableString
                                        + "\n    ) "
                                        + "\n    SHARD COUNT " + shardCount + " ";
                            }
                        }
                    } catch (SQLException e) {
                        throw new DBException(e, dataSource);
                    }

                    try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT DISTINCT "
                            + "        tab.SHARD_PLACEMENT      "
                            + "      , cgrp.GROUP_NAME          "
                            + "      , cgrp.GROUP_ID            "
                            + "   FROM "
                            + "        DEFINITION_SCHEMA.TABLES@LOCAL            AS tab  "
                            + "      , DEFINITION_SCHEMA.SHARD_HASH@LOCAL        AS shd "
                            + "      , DEFINITION_SCHEMA.CLUSTER_GROUP@LOCAL     AS cgrp "
                            + "  WHERE "
                            + "        tab.TABLE_ID    = shd.TABLE_ID "
                            + "    AND shd.GROUP_ID   = cgrp.GROUP_ID "
                            + "    AND tab.TABLE_ID    = ? "
                            + "  ORDER BY "
                            + "        cgrp.GROUP_ID ")) {
                        dbStat.setString(1, tableId);
                        String placement = "";
                        String groupName = "";
                        boolean isFirst = true;
                        try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                            while (dbResult.nextRow()) {
                                placement = dbResult.getString(1);
                                groupName = dbResult.getString(2);

                                if (placement.contains("AT CLUSTER WIDE")) {
                                    tableString = tableString
                                            + "\n    AT CLUSTER WIDE ";
                                    break;
                                } else {
                                    if (isFirst == true) {
                                        isFirst = false;
                                        tableString = tableString
                                                + "\n    AT CLUSTER GROUP "
                                                + "\n        \"" + groupName + "\" ";
                                    } else {
                                        tableString = tableString
                                                + "\n      , \"" + groupName + "\" ";
                                    }
                                }
                            }
                        }
                    } catch (SQLException e) {
                        throw new DBException(e, dataSource);
                    }
                } else if (shardingStrategy.contains("RANGE SHARDING")) {
                    try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                            + "        col.COLUMN_NAME          "
                            + "   FROM "
                            + "        DEFINITION_SCHEMA.TABLES@LOCAL                   AS tab  "
                            + "      , DEFINITION_SCHEMA.SHARD_KEY_COLUMN_USAGE@LOCAL   AS skey  "
                            + "      , DEFINITION_SCHEMA.COLUMNS@LOCAL                  AS col  "
                            + "  WHERE "
                            + "        tab.TABLE_ID    = skey.TABLE_ID "
                            + "    AND skey.COLUMN_ID  = col.COLUMN_ID "
                            + "    AND tab.TABLE_ID    = ? "
                            + "  ORDER BY "
                            + "        skey.ORDINAL_POSITION ")) {
                        dbStat.setString(1, tableId);
                        String columnName = "";
                        boolean isFirst = true;
                        boolean hasData = false;
                        try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                            while (dbResult.nextRow()) {
                                hasData = true;
                                columnName = dbResult.getString(1);

                                if (isFirst == true) {
                                    isFirst = false;
                                    tableString = tableString
                                            + "\n    SHARDING BY RANGE "
                                            + "\n    ( "
                                            + "\n        \"" + columnName + "\" ";
                                } else {
                                    tableString = tableString
                                            + "\n      , \"" + columnName + "\" ";
                                }
                            }
                            if (hasData == true) {
                                tableString = tableString
                                        + "\n    ) ";
                            }
                        }
                    } catch (SQLException e) {
                        throw new DBException(e, dataSource);
                    }
                    try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT  "
                            + "        tab.SHARD_PLACEMENT      "
                            + "      , shd.SHARD_NAME           "
                            + "      , shd.VALUE_STRING         "
                            + "      , cgrp.GROUP_NAME          "
                            + "   FROM "
                            + "        DEFINITION_SCHEMA.TABLES@LOCAL            AS tab  "
                            + "      , DEFINITION_SCHEMA.SHARD_RANGE@LOCAL       AS shd "
                            + "      , DEFINITION_SCHEMA.CLUSTER_GROUP@LOCAL     AS cgrp "
                            + "  WHERE "
                            + "        tab.TABLE_ID    = shd.TABLE_ID "
                            + "    AND shd.GROUP_ID    = cgrp.GROUP_ID "
                            + "    AND tab.TABLE_ID    = ? "
                            + "  ORDER BY "
                            + "        shd.SHARD_NO ")) {
                        dbStat.setString(1, tableId);
                        String placement = "";
                        String shardName = "";
                        String valueStringLong = "";
                        String groupName = "";
                        boolean isFirst = true;
                        try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                            while (dbResult.nextRow()) {
                                placement = dbResult.getString(1);
                                shardName = dbResult.getString(2);
                                valueStringLong = dbResult.getString(3);
                                groupName = dbResult.getString(4);

                                if (placement.contains("AT CLUSTER WIDE")) {
                                    if (isFirst == true) {
                                        isFirst = false;
                                        tableString = tableString
                                                + "\n    AT CLUSTER WIDE "
                                                + "\n        SHARD \"" + shardName + "\" VALUES LESS THAN " + valueStringLong + " ";
                                    } else {
                                        tableString = tableString
                                                + "\n      , SHARD \"" + shardName + "\" VALUES LESS THAN " + valueStringLong + " ";
                                    }
                                } else {
                                    if (isFirst == true) {
                                        isFirst = false;
                                        tableString = tableString
                                                + "\n        SHARD \"" + shardName + "\" VALUES LESS THAN " + valueStringLong + " AT CLUSTER GROUP \"" + groupName + "\" ";
                                    } else {
                                        tableString = tableString
                                                + "\n      , SHARD \"" + shardName + "\" VALUES LESS THAN " + valueStringLong + " AT CLUSTER GROUP \"" + groupName + "\" ";
                                    }
                                }
                            }
                        }
                    } catch (SQLException e) {
                        throw new DBException(e, dataSource);
                    }
                } else if (shardingStrategy.contains("LIST SHARDING")) {
                    try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                            + "        col.COLUMN_NAME          "
                            + "   FROM "
                            + "        DEFINITION_SCHEMA.TABLES@LOCAL                   AS tab  "
                            + "      , DEFINITION_SCHEMA.SHARD_KEY_COLUMN_USAGE@LOCAL   AS skey  "
                            + "      , DEFINITION_SCHEMA.COLUMNS@LOCAL                  AS col  "
                            + "  WHERE "
                            + "        tab.TABLE_ID    = skey.TABLE_ID "
                            + "    AND skey.COLUMN_ID  = col.COLUMN_ID "
                            + "    AND tab.TABLE_ID    = ? "
                            + "  ORDER BY "
                            + "        skey.ORDINAL_POSITION ")) {
                        dbStat.setString(1, tableId);
                        String columnName = "";
                        boolean isFirst = true;
                        boolean hasData = false;
                        try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                            while (dbResult.nextRow()) {
                                hasData = true;
                                columnName = dbResult.getString(1);

                                if (isFirst == true) {
                                    isFirst = false;
                                    tableString = tableString 
                                            + "\n    SHARDING BY LIST "
                                            + "\n    ( "
                                            + "\n        \"" + columnName + "\" ";
                                } else {
                                    tableString = tableString
                                            + "\n      , \"" + columnName + "\" ";
                                }
                            }
                            if (hasData == true) {
                                tableString = tableString
                                        + "\n    ) ";
                            }
                        }
                    } catch (SQLException e) {
                        throw new DBException(e, dataSource);
                    }
                    try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT  "
                            + "        tab.SHARD_PLACEMENT      "
                            + "      , shd.SHARD_NAME           "
                            + "      , shd.VALUE_STRING         "
                            + "      , cgrp.GROUP_NAME          "
                            + "   FROM "
                            + "        DEFINITION_SCHEMA.TABLES@LOCAL            AS tab  "
                            + "      , DEFINITION_SCHEMA.SHARD_LIST@LOCAL        AS shd "
                            + "      , DEFINITION_SCHEMA.CLUSTER_GROUP@LOCAL     AS cgrp "
                            + "  WHERE "
                            + "        tab.TABLE_ID    = shd.TABLE_ID "
                            + "    AND shd.GROUP_ID    = cgrp.GROUP_ID "
                            + "    AND tab.TABLE_ID    = ? "
                            + "  ORDER BY "
                            + "        shd.SHARD_NO ")) {
                        dbStat.setString(1, tableId);
                        String placement = "";
                        String shardName = "";
                        String valueStringLong = "";
                        String groupName = "";
                        boolean isFirst = true;
                        try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                            while (dbResult.nextRow()) {
                                placement = dbResult.getString(1);
                                shardName = dbResult.getString(2);
                                valueStringLong = dbResult.getString(3);
                                groupName = dbResult.getString(4);

                                if (placement.contains("AT CLUSTER WIDE")) {
                                    if (isFirst == true) {
                                        isFirst = false;
                                        tableString = tableString
                                                + "\n    AT CLUSTER WIDE "
                                                + "\n        SHARD \"" + shardName + "\" VALUES IN " + valueStringLong + " ";
                                    } else {
                                        tableString = tableString
                                                + "\n      , SHARD \"" + shardName + "\" VALUES IN " + valueStringLong + " ";
                                    }
                                } else {
                                    if (isFirst == true) {
                                        isFirst = false;
                                        tableString = tableString
                                                + "\n        SHARD \"" + shardName + "\" VALUES IN " + valueStringLong + " AT CLUSTER GROUP \"" + groupName + "\" ";
                                    } else {
                                        tableString = tableString
                                                + "\n      , SHARD \"" + shardName + "\" VALUES IN " + valueStringLong + " AT CLUSTER GROUP \"" + groupName + "\" ";
                                    }
                                }
                            }
                        }
                    } catch (SQLException e) {
                        throw new DBException(e, dataSource);
                    }
                }
            }

            if (cacheTableType.compareToIgnoreCase("HEAP_COLUMNAR") == 0) {
                tableString = tableString
                        + "\n    WITH COLUMNAR OPTIONS ";
            } else {
                tableString = tableString
                        + "\n    PCTFREE  " + pctFree + " "
                        + "\n    PCTUSED  " + pctUsed + " "
                        + "\n    INITRANS " + iniTrans + " "
                        + "\n    MAXTRANS " + maxTrans + " ";
            }

            tableString = tableString
                    + "\n    STORAGE "
                    + "\n    ( "
                    + "\n        INITIAL " + segInit + " "
                    + "\n        NEXT    " + segNext + " "
                    + "\n        MINSIZE " + segMin + " "
                    + "\n        MAXSIZE " + segMax + " "
                    + "\n    ) ";

            tableString = tableString
                    + "\n    TABLESPACE \"" + spaceName + "\" ";

            if (nullShardingStrategy == false) {
                tableString = tableString
                        + "\n    WITHOUT GLOBAL SECONDARY INDEX ";
            }
        }

        tableString = tableString
                + "\n;\n"
                + "COMMIT;\n";
        
        return tableString;
    }
    
    private String getNotNullConstraint(GenericDataSource dataSource, JDBCSession session, int constId)
            throws DBException {
        String constraintDDL = "";
        
        try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                + "        auth.AUTHORIZATION_NAME  "
                + "      , tsch.SCHEMA_NAME         "
                + "      , tab.TABLE_NAME           "
                + "      , csch.SCHEMA_NAME         "
                + "      , cst.CONSTRAINT_NAME      "
                + "      , col.COLUMN_NAME          "
                + "      , CAST ( CASE WHEN cst.IS_DEFERRABLE = TRUE THEN 'YES' "
                + "                                                  ELSE 'NO' "
                + "                     END AS VARCHAR(3 OCTETS) ) "
                + "      , CAST ( CASE WHEN cst.INITIALLY_DEFERRED = TRUE THEN 'YES' "
                + "                                                       ELSE 'NO' "
                + "                     END AS VARCHAR(3 OCTETS) ) "
                + "   FROM "
                + "        DEFINITION_SCHEMA.TABLE_CONSTRAINTS@LOCAL     AS cst "
                + "      , DEFINITION_SCHEMA.CHECK_COLUMN_USAGE@LOCAL    AS ccu "
                + "      , DEFINITION_SCHEMA.TABLES@LOCAL                AS tab  "
                + "      , DEFINITION_SCHEMA.COLUMNS@LOCAL               AS col  "
                + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL              AS csch  "
                + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL              AS tsch  "
                + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL        AS auth  "
                + "  WHERE "
                + "        cst.CONSTRAINT_ID        = ccu.CONSTRAINT_ID "
                + "    AND cst.CONSTRAINT_SCHEMA_ID = csch.SCHEMA_ID "
                + "    AND cst.CONSTRAINT_OWNER_ID  = auth.AUTH_ID "
                + "    AND cst.TABLE_SCHEMA_ID      = tsch.SCHEMA_ID "
                + "    AND cst.TABLE_ID             = tab.TABLE_ID "
                + "    AND ccu.COLUMN_ID            = col.COLUMN_ID "
                + "    AND cst.CONSTRAINT_ID        = ? ")) {
            dbStat.setInt(1, constId);

            String constOwnerName = "";
            String tableSchemaName = "";
            String tableName = "";
            String constSchemaName = "";
            String constName = "";
            String columnName = "";
            String deferrable = "";
            String initDeferred = "";

            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    constOwnerName = dbResult.getString(1);
                    tableSchemaName = dbResult.getString(2);
                    tableName = dbResult.getString(3);
                    constSchemaName = dbResult.getString(4);
                    constName = dbResult.getString(5);
                    columnName = dbResult.getString(6);
                    deferrable = dbResult.getString(7);
                    initDeferred = dbResult.getString(8);
                }
            }
            
            constraintDDL = constraintDDL
                    + "\nSET SESSION AUTHORIZATION \"" + constOwnerName + "\"; "
                    + "\nALTER TABLE \"" + tableSchemaName + "\".\"" + tableName + "\" "
                    + "\n    ALTER COLUMN \"" + columnName + "\" "
                    + "\n    SET CONSTRAINT \"" + constSchemaName + "\".\"" + constName + "\" NOT NULL ";
            
            if (deferrable.contains("YES")) {
                constraintDDL = constraintDDL
                        + "\n    DEFERRABLE ";
            } else {
                constraintDDL = constraintDDL
                        + "\n    NOT DEFERRABLE "; 
            }
            
            if (initDeferred.contains("YES")) {
                constraintDDL = constraintDDL
                        + "\n    INITIALLY DEFERRED ";
            } else {
                constraintDDL = constraintDDL
                        + "\n    INITIALLY IMMEDIATE ";
            }            
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
        
        constraintDDL = constraintDDL
                + "\n;\n"
                + "COMMIT;\n";
        
        return constraintDDL;
    }

    private String getUniquePrimaryConstraint(GenericDataSource dataSource, JDBCSession session, int constId)
            throws DBException {
        String constraintDDL = "";
        
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "        auth.AUTHORIZATION_NAME  "
                        + "      , tsch.SCHEMA_NAME         "
                        + "      , tab.TABLE_NAME           "
                        + "      , tab.TABLE_TYPE           "
                        + "      , csch.SCHEMA_NAME         "
                        + "      , cst.CONSTRAINT_NAME      "
                        + "      , cst.CONSTRAINT_TYPE      "
                        + "      , isch.SCHEMA_NAME         "
                        + "      , idx.INDEX_NAME           "
                        + "      , iphy.PCTFREE             "
                        + "      , iphy.INITRANS            "
                        + "      , iphy.MAXTRANS            "
                        + "      , ( iphy.INITIAL_EXTENTS * xspc.EXTSIZE ) "
                        + "      , ( iphy.NEXT_EXTENTS * xspc.EXTSIZE )    "
                        + "      , ( iphy.MIN_EXTENTS * xspc.EXTSIZE )     "
                        + "      , ( iphy.MAX_EXTENTS * xspc.EXTSIZE )     "
                        + "      , CAST( CASE WHEN iphy.IS_LOGGING = TRUE "
                        + "                   THEN 'YES' "
                        + "                   ELSE 'NO' "
                        + "                   END "
                        + "               AS VARCHAR(3 OCTETS) ) "
                        + "      , spc.TABLESPACE_NAME      "
                        + "      , CAST ( CASE WHEN cst.IS_DEFERRABLE = TRUE THEN 'YES' "
                        + "                                                  ELSE 'NO' "
                        + "                     END AS VARCHAR(3 OCTETS) ) "
                        + "      , CAST ( CASE WHEN cst.INITIALLY_DEFERRED = TRUE THEN 'YES' "
                        + "                                                       ELSE 'NO' "
                        + "                     END AS VARCHAR(3 OCTETS) ) "
                        + "   FROM "
                        + "        DEFINITION_SCHEMA.TABLE_CONSTRAINTS@LOCAL     AS cst "
                        + "      , DEFINITION_SCHEMA.TABLES@LOCAL                AS tab  "
                        + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL              AS csch  "
                        + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL              AS tsch  "
                        + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL        AS auth  "
                        + "      , (DEFINITION_SCHEMA.INDEXES@LOCAL               AS idx "
                        + "         INNER JOIN DEFINITION_SCHEMA.SCHEMATA@LOCAL              AS isch  "
                        + "             ON idx.SCHEMA_ID            = isch.SCHEMA_ID) "
                        + "        LEFT OUTER JOIN DEFINITION_SCHEMA.TABLESPACES@LOCAL   AS spc   "
                        + "              ON idx.TABLESPACE_ID        = spc.TABLESPACE_ID "
                        + "        LEFT OUTER JOIN FIXED_TABLE_SCHEMA.X$TABLESPACE@LOCAL AS xspc   "
                        + "              ON idx.TABLESPACE_ID        = xspc.ID "
                        + "        LEFT OUTER JOIN "
                        + "        ( "
                        + "          SELECT "
                        + "                 sch.SCHEMA_NAME             AS index_schema "
                        + "               , idx.INDEX_NAME              AS index_name "
                        + "               , MAX( xidx.PCTFREE )         AS pctfree "
                        + "               , MAX( xidx.INITRANS )        AS initrans "
                        + "               , MAX( xidx.MAXTRANS )        AS maxtrans "
                        + "               , MAX( xseg.INITIAL_EXTENTS ) AS initial_extents "
                        + "               , MAX( xseg.NEXT_EXTENTS )    AS next_extents "
                        + "               , MAX( xseg.MIN_EXTENTS )     AS min_extents "
                        + "               , MAX( xseg.MAX_EXTENTS )     AS max_extents "
                        + "               , MAX( xidx.IS_LOGGING )      AS is_logging "
                        + "            FROM "
                        + "                 DEFINITION_SCHEMA.SCHEMATA@GLOBAL[IGNORE_INACTIVE_MEMBER]         AS sch "
                        + "               , DEFINITION_SCHEMA.INDEXES@GLOBAL[IGNORE_INACTIVE_MEMBER]          AS idx "
                        + "               , FIXED_TABLE_SCHEMA.X$INDEX_HEADER@GLOBAL[IGNORE_INACTIVE_MEMBER]  AS xidx "
                        + "               , FIXED_TABLE_SCHEMA.X$SEGMENT@GLOBAL[IGNORE_INACTIVE_MEMBER]       AS xseg "
                        + "           WHERE "
                        + "                 idx.CLUSTER_MEMBER_ID  = sch.CLUSTER_MEMBER_ID "
                        + "             AND idx.SCHEMA_ID          = sch.SCHEMA_ID "
                        + "             AND idx.CLUSTER_MEMBER_ID  = xidx.CLUSTER_MEMBER_ID "
                        + "             AND idx.PHYSICAL_ID        = xidx.PHYSICAL_ID "
                        + "             AND idx.CLUSTER_MEMBER_ID  = xseg.CLUSTER_MEMBER_ID "
                        + "             AND idx.PHYSICAL_ID        = xseg.PHYSICAL_ID "
                        + "           GROUP BY "
                        + "                 sch.SCHEMA_NAME "
                        + "               , idx.INDEX_NAME "
                        + "        ) AS iphy " 
                        + "            ON     isch.SCHEMA_NAME         = iphy.INDEX_SCHEMA "
                        + "               AND idx.INDEX_NAME           = iphy.INDEX_NAME "
                        + "  WHERE "
                        + "        cst.CONSTRAINT_SCHEMA_ID = csch.SCHEMA_ID "
                        + "    AND cst.CONSTRAINT_OWNER_ID  = auth.AUTH_ID "
                        + "    AND cst.TABLE_SCHEMA_ID      = tsch.SCHEMA_ID "
                        + "    AND cst.TABLE_ID             = tab.TABLE_ID "
                        + "    AND cst.ASSOCIATED_INDEX_ID  = idx.INDEX_ID "
                        + "    AND idx.SCHEMA_ID            = isch.SCHEMA_ID "
                        + "    AND cst.CONSTRAINT_ID        = ? ")) {
            dbStat.setInt(1, constId);

            String constOwnerName = "";
            String tableSchemaName = "";
            String tableName = "";
            String tableType = "";
            String constSchemaName = "";
            String constName = "";
            String constType = "";
            String indexName = "";
            String pctFree = "";
            String iniTrans = "";
            String maxTrans = "";
            String segInit = "";
            String segNext = "";
            String segMin = "";
            String segMax = "";
            String spaceName = "";
            boolean nullSpaceName = false;
            String deferrable = "";
            String initDeferred = "";

            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    constOwnerName = dbResult.getString(1);
                    tableSchemaName = dbResult.getString(2);
                    tableName = dbResult.getString(3);
                    tableType = dbResult.getString(4);
                    constSchemaName = dbResult.getString(5);
                    constName = dbResult.getString(6);
                    constType = dbResult.getString(7);
                    indexName = dbResult.getString(9);
                    pctFree = dbResult.getString(10);
                    iniTrans = dbResult.getString(11);
                    maxTrans = dbResult.getString(12);
                    segInit = dbResult.getString(13);
                    segNext = dbResult.getString(14);
                    segMin = dbResult.getString(15);
                    segMax = dbResult.getString(16);
                    spaceName = dbResult.getString(18);
                    nullSpaceName = dbResult.wasNull();
                    deferrable = dbResult.getString(19);
                    initDeferred = dbResult.getString(20);
                }
            }
            
            constraintDDL = constraintDDL
                  + "\nSET SESSION AUTHORIZATION \"" + constOwnerName + "\"; "
                  + "\nALTER TABLE \"" + tableSchemaName + "\".\"" + tableName + "\" "
                  + "\n    ADD CONSTRAINT \"" + constSchemaName + "\".\"" + constName + "\" "
                  + "\n    " + constType + " ";
            
            try (JDBCPreparedStatement dbStat2 = session
                    .prepareStatement("SELECT "
                            + "        col.COLUMN_NAME "
                            + "      , CAST( CASE WHEN ikey.IS_ASCENDING_ORDER = TRUE "
                            + "                   THEN 'ASC' "
                            + "                   ELSE 'DESC' "
                            + "                   END "
                            + "                AS VARCHAR(32 OCTETS) ) "
                            + "      , CAST( CASE WHEN ikey.IS_NULLS_FIRST = TRUE "
                            + "                   THEN 'NULLS FIRST' "
                            + "                   ELSE 'NULLS LAST' "
                            + "                   END "
                            + "                AS VARCHAR(32 OCTETS) ) "
                            + "   FROM "
                            + "        DEFINITION_SCHEMA.INDEX_KEY_COLUMN_USAGE@LOCAL AS ikey "
                            + "      , DEFINITION_SCHEMA.TABLE_CONSTRAINTS@LOCAL      AS cst  "
                            + "      , DEFINITION_SCHEMA.COLUMNS@LOCAL                AS col  "
                            + "  WHERE      "
                            + "        cst.ASSOCIATED_INDEX_ID = ikey.INDEX_ID  "
                            + "    AND ikey.COLUMN_ID          = col.COLUMN_ID "
                            + "    AND cst.CONSTRAINT_ID       = ? "
                            + "  ORDER BY "
                            + "        ikey.ORDINAL_POSITION ")) {
                dbStat2.setInt(1, constId);

                String columnName = "";
                String isAsc = "";
                String isNullsFirst = "";
                int colCount = 0;
                try (JDBCResultSet dbResult = dbStat2.executeQuery()) {
                    while (dbResult.nextRow()) {
                        columnName = dbResult.getString(1);
                        isAsc = dbResult.getString(2);
                        isNullsFirst = dbResult.getString(3);

                        if (colCount == 0) {
                            constraintDDL = constraintDDL
                                    + "\n        \"" + columnName + "\" " + isAsc + " " + isNullsFirst; 
                        } else {
                            constraintDDL = constraintDDL
                                    + "\n      , \"" + columnName + "\" " + isAsc + " " + isNullsFirst;
                        }
                        colCount++;
                    }
                    constraintDDL = constraintDDL
                            + "\n    ) ";
                }
            }
            
            constraintDDL = constraintDDL
                    + "\n    INDEX \"" + indexName + "\" ";
            
            if (tableType.contains("GLOBAL TEMPORARY")) {
                if (nullSpaceName == false) {
                    constraintDDL = constraintDDL
                            + "\n    TABLESPACE \"" + spaceName + "\" ";
                }   
            } else {
                constraintDDL = constraintDDL
                        + "\n    PCTFREE  " + pctFree + " "
                        + "\n    INITRANS " + iniTrans + " "
                        + "\n    MAXTRANS " + maxTrans + " "
                        + "\n    STORAGE "
                        + "\n    ( "
                        + "\n        INITIAL " + segInit + " "
                        + "\n        NEXT    " + segNext + " "
                        + "\n        MINSIZE " + segMin + " "
                        + "\n        MAXSIZE " + segMax + " "
                        + "\n    ) "
                        + "\n    TABLESPACE \"" + spaceName + "\" ";
            }   
            
            if (deferrable.contains("YES")) {
                constraintDDL = constraintDDL
                        + "\n    DEFERRABLE ";
            } else {
                constraintDDL = constraintDDL
                        + "\n    NOT DEFERRABLE ";
            }
            
            if (initDeferred.contains("YES")) {
                constraintDDL = constraintDDL
                        + "\n    INITIALLY DEFERRED ";
            } else {
                constraintDDL = constraintDDL
                        + "\n    INITIALLY IMMEDIATE ";
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
        
        constraintDDL = constraintDDL
                + "\n;\n"
                + "COMMIT;\n";

        return constraintDDL;
    }

    private String getAddConstraintString(GenericDataSource dataSource, JDBCSession session, int constId)
            throws DBException {
        String constraintDDL = "";
        
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "       const.CONSTRAINT_TYPE "
                        + "  FROM "
                        + "       DEFINITION_SCHEMA.TABLE_CONSTRAINTS@LOCAL const "
                        + " WHERE "
                        + "       const.CONSTRAINT_ID = ? ")) {
            dbStat.setInt(1, constId);

            String constType = "";
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    constType = dbResult.getString(1);

                    if (constType.contains("NOT NULL")) {
                        constraintDDL = getNotNullConstraint(dataSource, session, constId);
                    } else if (constType.contains("PRIMARY KEY")) {
                        constraintDDL = getUniquePrimaryConstraint(dataSource, session, constId);
                    } else if (constType.contains("UNIQUE")) {
                        constraintDDL = getUniquePrimaryConstraint(dataSource, session, constId);
                    }
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
        return constraintDDL;
    }
    
    private String getTableConstraint(GenericDataSource dataSource, JDBCSession session, String tableId)
            throws DBException {
        String constraintDDL = "";
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "       CONSTRAINT_ID "
                        + "  FROM "
                        + "       DEFINITION_SCHEMA.TABLE_CONSTRAINTS@LOCAL "
                        + " WHERE "
                        + "       CONSTRAINT_TYPE IN ( 'NOT NULL', 'PRIMARY KEY', 'UNIQUE' ) "
                        + "   AND TABLE_ID = ? "
                        + " ORDER BY  "
                        + "       CASE WHEN CONSTRAINT_TYPE = 'NOT NULL'    THEN 1 "
                        + "            WHEN CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 2 "
                        + "            WHEN CONSTRAINT_TYPE = 'UNIQUE'      THEN 3 "
                        + "            ELSE NULL "
                        + "            END "
                        +"     , CONSTRAINT_ID ")) {
            dbStat.setString(1, tableId);

            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                while (dbResult.nextRow()) {
                    constraintDDL = constraintDDL + getAddConstraintString(dataSource, session, dbResult.getInt(1));
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
        return constraintDDL;
    }

    private String getTableGlobalSeconaryIndex(GenericDataSource dataSource, JDBCSession session, String tableId)
            throws DBException {
        String globalSeconaryIndexDDL = "";
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "        auth.AUTHORIZATION_NAME "
                        + "      , sch.SCHEMA_NAME "        
                        + "      , tab.TABLE_NAME "           
                        + "      , iphy.PCTFREE "             
                        + "      , iphy.INITRANS "            
                        + "      , iphy.MAXTRANS "            
                        + "      , ( iphy.INITIAL_EXTENTS * xspc.EXTSIZE ) " 
                        + "      , ( iphy.NEXT_EXTENTS * xspc.EXTSIZE ) "    
                        + "      , ( iphy.MIN_EXTENTS * xspc.EXTSIZE ) "     
                        + "      , ( iphy.MAX_EXTENTS * xspc.EXTSIZE ) "     
                        + "      , CAST( CASE WHEN iphy.IS_LOGGING = TRUE  "
                        + "                   THEN 'YES' " 
                        + "                   ELSE 'NO' " 
                        + "                   END " 
                        + "               AS VARCHAR(3 OCTETS) ) " 
                        + "      , spc.TABLESPACE_NAME "       
                        + "   FROM  "
                        + "        DEFINITION_SCHEMA.TABLES@LOCAL                AS tab "  
                        + "      , DEFINITION_SCHEMA.TABLESPACES@LOCAL           AS spc "   
                        + "      , FIXED_TABLE_SCHEMA.X$TABLESPACE@LOCAL         AS xspc "   
                        + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL              AS sch "  
                        + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL        AS auth "  
                        + "      , (  "
                        + "          SELECT " 
                        + "                 sch.SCHEMA_NAME             AS table_schema " 
                        + "               , tab.TABLE_NAME              AS table_name " 
                        + "               , MAX( xidx.PCTFREE )         AS pctfree " 
                        + "               , MAX( xidx.INITRANS )        AS initrans " 
                        + "               , MAX( xidx.MAXTRANS )        AS maxtrans " 
                        + "               , MAX( xseg.INITIAL_EXTENTS ) AS initial_extents " 
                        + "               , MAX( xseg.NEXT_EXTENTS )    AS next_extents " 
                        + "               , MAX( xseg.MIN_EXTENTS )     AS min_extents " 
                        + "               , MAX( xseg.MAX_EXTENTS )     AS max_extents " 
                        + "               , MAX( xidx.IS_LOGGING )      AS is_logging " 
                        + "            FROM  "
                        + "                 DEFINITION_SCHEMA.SCHEMATA@GLOBAL[IGNORE_INACTIVE_MEMBER]         AS sch " 
                        + "               , DEFINITION_SCHEMA.TABLES@GLOBAL[IGNORE_INACTIVE_MEMBER]           AS tab " 
                        + "               , FIXED_TABLE_SCHEMA.X$INDEX_HEADER@GLOBAL[IGNORE_INACTIVE_MEMBER]  AS xidx " 
                        + "               , FIXED_TABLE_SCHEMA.X$SEGMENT@GLOBAL[IGNORE_INACTIVE_MEMBER]       AS xseg " 
                        + "           WHERE tab.HAS_GSI = TRUE "
                        + "             AND tab.CLUSTER_MEMBER_ID  = sch.CLUSTER_MEMBER_ID " 
                        + "             AND tab.SCHEMA_ID          = sch.SCHEMA_ID " 
                        + "             AND tab.CLUSTER_MEMBER_ID  = xidx.CLUSTER_MEMBER_ID " 
                        + "             AND tab.GSI_PHYSICAL_ID    = xidx.PHYSICAL_ID " 
                        + "             AND tab.CLUSTER_MEMBER_ID  = xseg.CLUSTER_MEMBER_ID " 
                        + "             AND tab.GSI_PHYSICAL_ID    = xseg.PHYSICAL_ID " 
                        + "           GROUP BY " 
                        + "                 sch.SCHEMA_NAME " 
                        + "               , tab.TABLE_NAME " 
                        + "        ) AS iphy "  
                        + "  WHERE tab.HAS_GSI = TRUE "
                        + "    AND tab.GSI_TABLESPACE_ID  = spc.TABLESPACE_ID " 
                        + "    AND tab.GSI_TABLESPACE_ID  = xspc.ID " 
                        + "    AND tab.SCHEMA_ID          = sch.SCHEMA_ID " 
                        + "    AND tab.OWNER_ID           = auth.AUTH_ID " 
                        + "    AND tab.TABLE_NAME         = iphy.TABLE_NAME "
                        + "    AND sch.SCHEMA_NAME        = iphy.TABLE_SCHEMA "
                        + "    AND tab.TABLE_ID           = ? ")) {
            dbStat.setString(1, tableId);

            String ownerName = "";
            String schemaName = "";
            String tableName = "";
            String pctFree = "";
            String iniTrans = "";
            String maxTrans = "";
            String segInit = "";
            String segNext = "";
            String segMin = "";
            String segMax = "";
            String spaceName = "";
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    ownerName = dbResult.getString(1);
                    schemaName = dbResult.getString(2);
                    tableName = dbResult.getString(3);
                    pctFree = dbResult.getString(4);
                    iniTrans = dbResult.getString(5);
                    maxTrans = dbResult.getString(6);
                    segInit = dbResult.getString(7);
                    segNext = dbResult.getString(8);
                    segMin = dbResult.getString(9);
                    segMax = dbResult.getString(10);
                    spaceName = dbResult.getString(12);
                    
                    globalSeconaryIndexDDL = globalSeconaryIndexDDL
                            + "\nSET SESSION AUTHORIZATION \"" + ownerName + "\"; "
                            + "\nALTER TABLE \"" + schemaName + "\".\"" + tableName + "\" ADD GLOBAL SECONDARY INDEX "
                            + "\n    PCTFREE  " + pctFree + " "
                            + "\n    INITRANS " + iniTrans + " "
                            + "\n    MAXTRANS " + maxTrans + " "
                            + "\n    STORAGE "
                            + "\n    ( "
                            + "\n        INITIAL " + segInit + " "
                            + "\n        NEXT    " + segNext + " "
                            + "\n        MINSIZE " + segMin + " "
                            + "\n        MAXSIZE " + segMax + " "
                            + "\n    ) "
                            + "\n    TABLESPACE \"" + spaceName + "\" "
                            + "\n;\n"
                            + "COMMIT;\n";
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }

        return globalSeconaryIndexDDL;
    }

    private String getCreateIndex(GenericDataSource dataSource, JDBCSession session, String indexId)
            throws DBException {
        String createIndexDDL = "";
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "        auth1.AUTHORIZATION_NAME  "
                        + "      , CAST( CASE WHEN idx.IS_UNIQUE = TRUE  "
                        + "                   THEN 'YES' "
                        + "                   ELSE 'NO' "
                        + "                   END "
                        + "              AS VARCHAR(3 OCTETS) )  "
                        + "      , sch1.SCHEMA_NAME         "
                        + "      , idx.INDEX_NAME           "
                        + "      , sch2.SCHEMA_NAME         "
                        + "      , tab.TABLE_NAME           "
                        + "      , tab.TABLE_TYPE           "
                        + "      , iphy.PCTFREE             "
                        + "      , iphy.INITRANS            "
                        + "      , iphy.MAXTRANS            "
                        + "      , ( iphy.INITIAL_EXTENTS * xspc.EXTSIZE ) "
                        + "      , ( iphy.NEXT_EXTENTS * xspc.EXTSIZE )    "
                        + "      , ( iphy.MIN_EXTENTS * xspc.EXTSIZE )     "
                        + "      , ( iphy.MAX_EXTENTS * xspc.EXTSIZE )     "
                        + "      , CAST( CASE WHEN iphy.IS_LOGGING = TRUE "
                        + "                   THEN 'YES' "
                        + "                   ELSE 'NO' "
                        + "                   END "
                        + "               AS VARCHAR(3 OCTETS) ) "
                        + "      , spc.TABLESPACE_NAME       "
                        + "   FROM "
                        + "        DEFINITION_SCHEMA.INDEXES@LOCAL                         AS idx "
                        + "          INNER JOIN DEFINITION_SCHEMA.SCHEMATA@LOCAL           AS sch1  "
                        + "              ON idx.SCHEMA_ID          = sch1.SCHEMA_ID "
                        + "          LEFT OUTER JOIN DEFINITION_SCHEMA.TABLESPACES@LOCAL   AS spc   "
                        + "              ON idx.TABLESPACE_ID      = spc.TABLESPACE_ID "
                        + "          LEFT OUTER JOIN FIXED_TABLE_SCHEMA.X$TABLESPACE@LOCAL AS xspc   "
                        + "              ON idx.TABLESPACE_ID      = xspc.ID "
                        + "          LEFT OUTER JOIN ( "
                        + "          SELECT "
                        + "                 sch.SCHEMA_NAME             AS index_schema "
                        + "               , idx.INDEX_NAME              AS index_name "
                        + "               , MAX( xidx.PCTFREE )         AS pctfree "
                        + "               , MAX( xidx.INITRANS )        AS initrans "
                        + "               , MAX( xidx.MAXTRANS )        AS maxtrans "
                        + "               , MAX( xseg.INITIAL_EXTENTS ) AS initial_extents "
                        + "               , MAX( xseg.NEXT_EXTENTS )    AS next_extents "
                        + "               , MAX( xseg.MIN_EXTENTS )     AS min_extents "
                        + "               , MAX( xseg.MAX_EXTENTS )     AS max_extents "
                        + "               , MAX( xidx.IS_LOGGING )      AS is_logging "
                        + "            FROM "
                        + "                 DEFINITION_SCHEMA.SCHEMATA@GLOBAL[IGNORE_INACTIVE_MEMBER]         AS sch "
                        + "               , DEFINITION_SCHEMA.INDEXES@GLOBAL[IGNORE_INACTIVE_MEMBER]          AS idx "
                        + "               , FIXED_TABLE_SCHEMA.X$INDEX_HEADER@GLOBAL[IGNORE_INACTIVE_MEMBER]  AS xidx "
                        + "               , FIXED_TABLE_SCHEMA.X$SEGMENT@GLOBAL[IGNORE_INACTIVE_MEMBER]       AS xseg "
                        + "           WHERE "
                        + "                 idx.CLUSTER_MEMBER_ID  = sch.CLUSTER_MEMBER_ID "
                        + "             AND idx.SCHEMA_ID          = sch.SCHEMA_ID "
                        + "             AND idx.CLUSTER_MEMBER_ID  = xidx.CLUSTER_MEMBER_ID "
                        + "             AND idx.PHYSICAL_ID        = xidx.PHYSICAL_ID "
                        + "             AND idx.CLUSTER_MEMBER_ID  = xseg.CLUSTER_MEMBER_ID "
                        + "             AND idx.PHYSICAL_ID        = xseg.PHYSICAL_ID "
                        + "           GROUP BY "
                        + "                 sch.SCHEMA_NAME "
                        + "               , idx.INDEX_NAME "
                        + "        ) AS iphy " 
                        + "            ON     sch1.SCHEMA_NAME       = iphy.INDEX_SCHEMA "
                        + "               AND idx.INDEX_NAME         = iphy.INDEX_NAME "
                        + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL        AS auth1  "
                        + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL              AS sch2  "
                        + "      , DEFINITION_SCHEMA.TABLES@LOCAL                AS tab  "
                        + "  WHERE "
                        + "        idx.TABLE_ID           = tab.TABLE_ID "
                        + "    AND idx.OWNER_ID           = auth1.AUTH_ID "
                        + "    AND idx.TABLE_SCHEMA_ID    = sch2.SCHEMA_ID "
                        + "    AND idx.INDEX_ID           = ? ")) {
            dbStat.setString(1, indexId);

            String ownerName = "";
            String isUnique = "";
            String indexSchemaName = "";
            String indexName = "";
            String tableSchemaName = "";
            String tableName = "";
            String tableType = "";
            String pctFree = "";
            String iniTrans = "";
            String maxTrans = "";
            String segInit = "";
            String segNext = "";
            String segMin = "";
            String segMax = "";
            String spaceName = "";
            boolean nullSpaceName = false;

            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    ownerName = dbResult.getString(1);
                    isUnique = dbResult.getString(2);
                    indexSchemaName = dbResult.getString(3);
                    indexName = dbResult.getString(4);
                    tableSchemaName = dbResult.getString(5);
                    tableName = dbResult.getString(6);
                    tableType = dbResult.getString(7);
                    pctFree = dbResult.getString(8);
                    iniTrans = dbResult.getString(9);
                    maxTrans = dbResult.getString(10);
                    segInit = dbResult.getString(11);
                    segNext = dbResult.getString(12);
                    segMin = dbResult.getString(13);
                    segMax = dbResult.getString(14);
                    spaceName = dbResult.getString(16);
                    nullSpaceName = dbResult.wasNull();
                }
            }

            createIndexDDL = createIndexDDL
                      + "\nSET SESSION AUTHORIZATION \"" + ownerName + "\"; ";
            
            if (isUnique.contains("YES")) {
                createIndexDDL = createIndexDDL
                        + "\nCREATE UNIQUE INDEX \"" + indexSchemaName + "\".\"" + indexName + "\" ";
            } else {
                createIndexDDL = createIndexDDL
                        + "\nCREATE INDEX \"" + indexSchemaName + "\".\"" + indexName + "\" ";
            }
            
            createIndexDDL = createIndexDDL
                    + "\n    ON \"" + tableSchemaName + "\".\"" + tableName + "\" ";
            
            try (JDBCPreparedStatement dbStat2 = session
                    .prepareStatement("SELECT "
                            + "        col.COLUMN_NAME "
                            + "      , CAST( CASE WHEN ikey.IS_ASCENDING_ORDER = TRUE "
                            + "                   THEN 'ASC' "
                            + "                   ELSE 'DESC' "
                            + "                   END "
                            + "                AS VARCHAR(32 OCTETS) ) "
                            + "      , CAST( CASE WHEN ikey.IS_NULLS_FIRST = TRUE "
                            + "                   THEN 'NULLS FIRST' "
                            + "                   ELSE 'NULLS LAST' "
                            + "                   END "
                            + "                AS VARCHAR(32 OCTETS) ) "
                            + "   FROM "
                            + "        DEFINITION_SCHEMA.INDEX_KEY_COLUMN_USAGE@LOCAL AS ikey "
                            + "      , DEFINITION_SCHEMA.INDEXES@LOCAL                AS idx  "
                            + "      , DEFINITION_SCHEMA.COLUMNS@LOCAL                AS col  "
                            + "  WHERE      "
                            + "        ikey.INDEX_ID  = idx.INDEX_ID  "
                            + "    AND ikey.COLUMN_ID = col.COLUMN_ID "
                            + "    AND idx.INDEX_ID   = ? "
                            + "  ORDER BY "
                            + "        ikey.ORDINAL_POSITION ")) {
                dbStat2.setString(1, indexId);

                String columnName = "";
                String isAsc = "";
                String isNullsFirst = "";
                int colCount = 0;
                try (JDBCResultSet dbResult = dbStat2.executeQuery()) {
                    createIndexDDL = createIndexDDL
                            + "\n    ( ";
                    
                    while (dbResult.nextRow()) {
                        columnName = dbResult.getString(1);
                        isAsc = dbResult.getString(2);
                        isNullsFirst = dbResult.getString(3);
                        
                        if (colCount == 0) {
                            createIndexDDL = createIndexDDL
                                    + "\n        \"" + columnName + "\" " + isAsc + " " + isNullsFirst;
                        } else {
                            createIndexDDL = createIndexDDL
                                    + "\n      , \"" + columnName + "\" " + isAsc + " " + isNullsFirst;
                        }
                        colCount++;
                    }
                    
                    createIndexDDL = createIndexDDL
                            + "\n    ) ";
                }
            }
            
            if (tableType.contains("GLOBAL TEMPORARY")) {
                if (nullSpaceName == false) {
                    createIndexDDL = createIndexDDL
                            + "\n    TABLESPACE \"" + spaceName + "\" ";
                }                
            } else {
                createIndexDDL = createIndexDDL
                        + "\n    PCTFREE  " + pctFree + " "
                        + "\n    INITRANS " + iniTrans + " "
                        + "\n    MAXTRANS " + maxTrans + " "
                        + "\n    STORAGE "
                        + "\n    ( "
                        + "\n        INITIAL " + segInit + " "
                        + "\n        NEXT    " + segNext + " "
                        + "\n        MINSIZE " + segMin + " "
                        + "\n        MAXSIZE " + segMax + " "
                        + "\n    ) "
                        + "\n    TABLESPACE \"" + spaceName + "\" ";
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
        
        createIndexDDL = createIndexDDL
                + "\n;\n"
                + "COMMIT;\n";
        
        return createIndexDDL;
    }

    private String getTableCreateIndex(GenericDataSource dataSource, JDBCSession session, String tableId)
            throws DBException {
        String createIndexDDL = "";
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "       idx.INDEX_ID "
                        + "  FROM "
                        + "       DEFINITION_SCHEMA.INDEXES@LOCAL idx"
                        + " WHERE "
                        + "       idx.BY_CONSTRAINT = FALSE "
                        + "   AND idx.TABLE_ID = ? "
                        + " ORDER BY "
                        + "       idx.INDEX_ID ")) {
            dbStat.setString(1, tableId);
            
            String indexId = "";
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                while (dbResult.nextRow()) {
                    indexId = dbResult.getString(1);
                    createIndexDDL = createIndexDDL + getCreateIndex(dataSource, session, indexId);
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
        return createIndexDDL;
    }

    private String getTableIdentity(GenericDataSource dataSource, JDBCSession session, String tableId)
            throws DBException {
        String IdentityDDL = "";
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement(" SELECT "
                        + "        auth.AUTHORIZATION_NAME   "
                        + "      , sch.SCHEMA_NAME           "
                        + "      , tab.TABLE_NAME            "
                        + "      , col.COLUMN_NAME           "
                        + "      , qphy.RESTART_VALUE        "
                        + "   FROM "
                        + "        DEFINITION_SCHEMA.TABLES@LOCAL           AS tab  "
                        + "      , DEFINITION_SCHEMA.COLUMNS@LOCAL          AS col  "
                        + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL         AS sch  "
                        + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL   AS auth  "
                        + "      , ( SELECT "
                        + "                 ssch.SCHEMA_NAME    AS schema_name "
                        + "               , stab.TABLE_NAME     AS table_name "
                        + "               , scol.COLUMN_NAME    AS column_name "
                        + "               , MAX( xsqc.RESTART_VALUE ) AS restart_value "
                        + "            FROM "
                        + "                 DEFINITION_SCHEMA.COLUMNS@GLOBAL[IGNORE_INACTIVE_MEMBER]           AS scol "
                        + "               , DEFINITION_SCHEMA.TABLES@GLOBAL[IGNORE_INACTIVE_MEMBER]            AS stab "
                        + "               , DEFINITION_SCHEMA.SCHEMATA@GLOBAL[IGNORE_INACTIVE_MEMBER]          AS ssch "
                        + "               , FIXED_TABLE_SCHEMA.X$SEQUENCE@GLOBAL[IGNORE_INACTIVE_MEMBER]       AS xsqc "
                        + "           WHERE "
                        + "                 scol.IS_IDENTITY = TRUE "
                        + "             AND scol.CLUSTER_MEMBER_ID    = ssch.CLUSTER_MEMBER_ID "
                        + "             AND scol.SCHEMA_ID            = ssch.SCHEMA_ID "
                        + "             AND scol.CLUSTER_MEMBER_ID    = stab.CLUSTER_MEMBER_ID "
                        + "             AND scol.TABLE_ID             = stab.TABLE_ID "
                        + "             AND scol.CLUSTER_MEMBER_ID    = xsqc.CLUSTER_MEMBER_ID "
                        + "             AND scol.IDENTITY_PHYSICAL_ID = xsqc.PHYSICAL_ID "
                        + "           GROUP BY "
                        + "                 ssch.SCHEMA_NAME "
                        + "               , stab.TABLE_NAME "
                        + "               , scol.COLUMN_NAME "
                        + "        ) AS qphy "
                        + "  WHERE "
                        + "        col.TABLE_ID             = tab.TABLE_ID "
                        + "    AND col.SCHEMA_ID            = sch.SCHEMA_ID "
                        + "    AND col.OWNER_ID             = auth.AUTH_ID "
                        + "    AND col.IS_IDENTITY          = TRUE "
                        + "    AND sch.SCHEMA_NAME          = qphy.SCHEMA_NAME "
                        + "    AND tab.TABLE_NAME           = qphy.TABLE_NAME "
                        + "    AND col.COLUMN_NAME          = qphy.COLUMN_NAME "
                        + "    AND tab.TABLE_ID             = ? ")) {
            dbStat.setString(1, tableId);

            String ownerName = "";
            String schemaName = "";
            String tableName = "";
            String columnName = "";
            String restartValue = "";
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    ownerName = dbResult.getString(1);
                    schemaName = dbResult.getString(2);
                    tableName = dbResult.getString(3);
                    columnName = dbResult.getString(4);
                    restartValue = dbResult.getString(5);
                    
                    IdentityDDL = IdentityDDL
                            + "\nSET SESSION AUTHORIZATION \"" + ownerName + "\"; "
                            + "\nALTER TABLE \"" + schemaName + "\".\"" + tableName + "\" "
                            + "\n    ALTER COLUMN \"" + columnName + "\" "
                            + "\n    RESTART WITH " + restartValue + " "
                            + "\n;\n"
                            + "COMMIT;\n";
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }

        return IdentityDDL;
    }

    private String getTableSupplemental(GenericDataSource dataSource, JDBCSession session, String tableId)
            throws DBException {
        String supplementalDDL = "";
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "        auth.AUTHORIZATION_NAME   "
                        + "      , sch.SCHEMA_NAME           "
                        + "      , tab.TABLE_NAME            "
                        + "   FROM "
                        + "        DEFINITION_SCHEMA.TABLES@LOCAL           AS tab  "
                        + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL         AS sch  "
                        + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL   AS auth  "
                        + "  WHERE "
                        + "        tab.SCHEMA_ID            = sch.SCHEMA_ID "
                        + "    AND tab.OWNER_ID             = auth.AUTH_ID "
                        + "    AND tab.IS_SET_SUPPLOG_PK    = TRUE "
                        + "    AND tab.TABLE_ID             = ? ")) {
            dbStat.setString(1, tableId);

            String ownerName = "";
            String schemaName = "";
            String tableName = "";
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    ownerName = dbResult.getString(1);
                    schemaName = dbResult.getString(2);
                    tableName = dbResult.getString(3);

                    supplementalDDL = supplementalDDL
                            + "\nSET SESSION AUTHORIZATION \"" + ownerName + "\"; "
                            + "\nALTER TABLE \"" + schemaName + "\".\"" + tableName + "\" "
                            + "\n    ADD SUPPLEMENTAL LOG DATA ( PRIMARY KEY ) COLUMNS "
                            + "\n;\n"
                            + "COMMIT;\n";
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }

        return supplementalDDL;
    }

    private LinkedList<PrivItem> buildPrivOrder(LinkedList<PrivItem> privItemList, long ownerId) {
        int order = 0;
        LinkedList<PrivItem> privOrderList = new LinkedList<PrivItem>();

        for (PrivItem item : privItemList) {
            if (item.granteeID == ownerId) {
                item.privOrder = order;
                order++;

                privOrderList.add(item);
            }
        }

        int loop = 0;
        int privCount = privItemList.size();

        while (order < privCount) {
            for (PrivItem childItem : privItemList) {
                if (childItem.privOrder == -1) {
                    for (PrivItem parentItem : privItemList) {
                        if (childItem.grantorID == parentItem.granteeID) {
                            if (parentItem.privOrder != -1) {
                                childItem.privOrder = order;
                                order++;
                                privOrderList.add(childItem);
                                break;
                            }
                        }
                    }
                }
            }

            if (loop > (privCount * privCount)) {
                for (PrivItem item : privItemList) {
                    if (item.privOrder == -1) {
                        item.privOrder = order;
                        order++;
                        privOrderList.add(item);
                    }
                }
            } else {
                loop++;
            }
        }
        return privOrderList;
    }

    private String getGrantByPrivOrder(LinkedList<PrivItem> privOrderList, String objectType, String privName,
            String nonSchemaName, String objectName, String columnName) {
        String grantDDL = "";
        for (PrivItem item : privOrderList) {
            if (item.isBuiltIn == true) {
                continue;
            }

            if (item.grantorName.compareTo("_SYSTEM") == 0) {
                grantDDL = grantDDL
                        + "\nSET SESSION AUTHORIZATION \"SYS\"; ";
            } else {
                grantDDL = grantDDL
                        + "\nSET SESSION AUTHORIZATION \"" + item.grantorName + "\"; ";
            }
            
            if (objectType.contains("DATABASE")) {
                grantDDL = grantDDL
                        + "\nGRANT "
                        + "\n    " + privName + " ON DATABASE "
                        + "\n    TO \"" + item.granteeName + "\" ";                
            } else if (objectType.contains("TABLESPACE")) {
                grantDDL = grantDDL
                        + "\nGRANT "
                        + "\n    " + privName + " ON TABLESPACE \"" + nonSchemaName + "\" "
                        + "\n    TO \"" + item.granteeName + "\" ";
            } else if (objectType.contains("SCHEMA")) {
                grantDDL = grantDDL
                        + "\nGRANT "
                        + "\n    " + privName + " ON SCHEMA \"" + nonSchemaName + "\" "
                        + "\n    TO \"" + item.granteeName + "\" ";
            } else if (objectType.contains("TABLE")) {
                grantDDL = grantDDL
                        + "\nGRANT "
                        + "\n    " + privName + " ON TABLE \"" + nonSchemaName + "\".\"" + objectName + "\" "
                        + "\n    TO \"" + item.granteeName + "\" ";
            } else if (objectType.contains("COLUMN")) {
                grantDDL = grantDDL
                        + "\nGRANT "
                        + "\n    " + privName + " ( \"" + columnName + "\" ) ON TABLE \"" + nonSchemaName + "\".\"" + objectName + "\" "
                        + "\n    TO \"" + item.granteeName + "\" ";
            } else if (objectType.contains("SEQUENCE")) {
                grantDDL = grantDDL
                        + "\nGRANT "
                        + "\n    USAGE ON SEQUENCE \"" + nonSchemaName + "\".\"" + objectName + "\" "
                        + "\n    TO \"" + item.granteeName + "\" ";
            } else if (objectType.contains("PROCEDURE")) {
                grantDDL = grantDDL
                        + "\nGRANT "
                        + "\n    EXECUTE ON PROCEDURE \"" + nonSchemaName + "\".\"" + objectName + "\" "
                        + "\n    TO \"" + item.granteeName + "\" ";
            } else if (objectType.contains("PACKAGE")) {
                grantDDL = grantDDL
                        + "\nGRANT "
                        + "\n    EXECUTE ON PACKAGE \"" + nonSchemaName + "\".\"" + objectName + "\" "
                        + "\n    TO \"" + item.granteeName + "\" ";
            }
            
            if (item.isGrantable == true) {
                grantDDL = grantDDL
                        + "\n    WITH GRANT OPTION ";
            }
            
            grantDDL = grantDDL
                    + "\n;\n"
                    + "COMMIT;\n";
        }
        
        return grantDDL;
    }
    
    private String getGrantRelation(GenericDataSource dataSource, JDBCSession session, String tableId)
            throws DBException {
        String GrantRelationDDL = "";
        String isBuiltIn = "NO";
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "       tab.OWNER_ID "
                        + "     , grantor.AUTH_ID "
                        + "     , grantor.AUTHORIZATION_NAME "
                        + "     , grantee.AUTH_ID "
                        + "     , grantee.AUTHORIZATION_NAME "
                        + "     , sch.SCHEMA_NAME "
                        + "     , tab.TABLE_NAME "
                        + "     , priv.PRIVILEGE_TYPE_ID "
                        + "     , priv.PRIVILEGE_TYPE "
                        + "     , CAST( CASE WHEN priv.IS_GRANTABLE = TRUE THEN 'YES' "
                        + "                                                ELSE 'NO' "
                        + "             END AS VARCHAR(3 OCTETS) ) "
                        + "  FROM "
                        + "       DEFINITION_SCHEMA.TABLES@LOCAL            AS tab "
                        + "     , DEFINITION_SCHEMA.TABLE_PRIVILEGES@LOCAL  AS priv "
                        + "     , DEFINITION_SCHEMA.SCHEMATA@LOCAL          AS sch "
                        + "     , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL    AS grantor "
                        + "     , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL    AS grantee "
                        + " WHERE "
                        + "       grantor.AUTHORIZATION_NAME <> '_SYSTEM' "
                        + "   AND grantee.AUTH_ID >= ( SELECT AUTH_ID "
                        + "                              FROM DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL "
                        + "                             WHERE AUTHORIZATION_NAME = 'PUBLIC' ) "
                        + "   AND tab.TABLE_ID     = priv.TABLE_ID "
                        + "   AND tab.SCHEMA_ID    = sch.SCHEMA_ID "
                        + "   AND priv.GRANTOR_ID  = grantor.AUTH_ID "
                        + "   AND priv.GRANTEE_ID  = grantee.AUTH_ID "
                        + "   AND tab.TABLE_ID = ? "
                        + " ORDER BY "
                        + "       priv.PRIVILEGE_TYPE_ID "
                        + "     , grantor.AUTH_ID "
                        + "     , grantee.AUTH_ID ")) {
            dbStat.setString(1, tableId);

            long ownerId = 0;
            long grantorID = 0;
            String grantorName = "";
            long granteeID = 0;
            String granteeName = "";
            String schemaName = "";
            String relName = "";
            long privType = 0;
            String privName = "";
            String grantable = "";

            long orgPrivType = 0;
            String orgPrivName = "";

            LinkedList<PrivItem> privItemList = new LinkedList<PrivItem>();

            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    ownerId = dbResult.getLong(1);
                    grantorID = dbResult.getLong(2);
                    grantorName = dbResult.getString(3);
                    granteeID = dbResult.getLong(4);
                    granteeName = dbResult.getString(5);
                    schemaName = dbResult.getString(6);
                    relName = dbResult.getString(7);
                    privType = dbResult.getLong(8);
                    privName = dbResult.getString(9);
                    grantable = dbResult.getString(10);

                    orgPrivType = privType;
                    orgPrivName = privName;

                    privItemList.add(new PrivItem(grantorID, grantorName, granteeID, granteeName,
                            isBuiltIn.contains("YES") ? true : false, grantable.contains("YES") ? true : false));

                    while (dbResult.nextRow()) {
                        ownerId = dbResult.getLong(1);
                        grantorID = dbResult.getLong(2);
                        grantorName = dbResult.getString(3);
                        granteeID = dbResult.getLong(4);
                        granteeName = dbResult.getString(5);
                        schemaName = dbResult.getString(6);
                        relName = dbResult.getString(7);
                        privType = dbResult.getLong(8);
                        privName = dbResult.getString(9);
                        grantable = dbResult.getString(10);

                        if (orgPrivType == privType) {
                            privItemList.add(new PrivItem(grantorID, grantorName, granteeID, granteeName,
                                    isBuiltIn.contains("YES") ? true : false,
                                    grantable.contains("YES") ? true : false));
                        } else {
                            LinkedList<PrivItem> privOrderList = buildPrivOrder(privItemList, ownerId);
                            GrantRelationDDL = GrantRelationDDL + getGrantByPrivOrder(privOrderList, "TABLE",
                                    orgPrivName, schemaName, relName, null);

                            orgPrivType = privType;
                            orgPrivName = privName;

                            privItemList.clear();
                            privItemList.add(new PrivItem(grantorID, grantorName, granteeID, granteeName,
                                    isBuiltIn.contains("YES") ? true : false,
                                    grantable.contains("YES") ? true : false));
                        }
                    }

                    LinkedList<PrivItem> privOrderList = buildPrivOrder(privItemList, ownerId);
                    GrantRelationDDL = GrantRelationDDL
                            + getGrantByPrivOrder(privOrderList, "TABLE", orgPrivName, schemaName, relName, null);
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
        
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "       tab.OWNER_ID "
                        + "     , grantor.AUTH_ID "
                        + "     , grantor.AUTHORIZATION_NAME "
                        + "     , grantee.AUTH_ID "
                        + "     , grantee.AUTHORIZATION_NAME "
                        + "     , sch.SCHEMA_NAME "
                        + "     , tab.TABLE_NAME "
                        + "     , col.COLUMN_ID "
                        + "     , col.COLUMN_NAME "
                        + "     , cpriv.PRIVILEGE_TYPE_ID "
                        + "     , cpriv.PRIVILEGE_TYPE "
                        + "     , CAST( CASE WHEN cpriv.IS_GRANTABLE = TRUE THEN 'YES' "
                        + "                                                 ELSE 'NO' "
                        + "             END AS VARCHAR(3 OCTETS) ) "
                        + "  FROM "
                        + "       DEFINITION_SCHEMA.TABLES@LOCAL             AS tab "
                        + "     , DEFINITION_SCHEMA.COLUMNS@LOCAL            AS col "
                        + "     , DEFINITION_SCHEMA.COLUMN_PRIVILEGES@LOCAL  AS cpriv "
                        + "     , DEFINITION_SCHEMA.SCHEMATA@LOCAL           AS sch "
                        + "     , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL     AS grantor "
                        + "     , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL     AS grantee "
                        + " WHERE "
                        + "       grantor.AUTHORIZATION_NAME <> '_SYSTEM' "
                        + "   AND grantee.AUTH_ID >= ( SELECT AUTH_ID "
                        + "                              FROM DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL "
                        + "                             WHERE AUTHORIZATION_NAME = 'PUBLIC' ) "
                        + "   AND tab.TABLE_ID      = col.TABLE_ID "
                        + "   AND col.COLUMN_ID     = cpriv.COLUMN_ID "
                        + "   AND tab.SCHEMA_ID     = sch.SCHEMA_ID "
                        + "   AND cpriv.GRANTOR_ID  = grantor.AUTH_ID "
                        + "   AND cpriv.GRANTEE_ID  = grantee.AUTH_ID "
                        + "   AND ( cpriv.PRIVILEGE_TYPE, cpriv.IS_GRANTABLE ) NOT IN "
                        + "       ( "
                        + "         SELECT tpriv.PRIVILEGE_TYPE, tpriv.IS_GRANTABLE "
                        + "           FROM "
                        + "                DEFINITION_SCHEMA.TABLES@LOCAL  AS subtab "
                        + "              , DEFINITION_SCHEMA.TABLE_PRIVILEGES@LOCAL  AS tpriv "
                        + "              , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL    AS subgrantor "
                        + "          WHERE "
                        + "                subgrantor.AUTHORIZATION_NAME <> '_SYSTEM' "
                        + "            AND subtab.TABLE_ID   = tab.TABLE_ID "
                        + "            AND subtab.TABLE_ID   = tpriv.TABLE_ID "
                        + "            AND tpriv.GRANTOR_ID  = subgrantor.AUTH_ID "
                        + "       ) "
                        + "   AND tab.TABLE_ID = ? "
                        + " ORDER BY "
                        + "       col.COLUMN_ID "
                        + "     , cpriv.PRIVILEGE_TYPE_ID "
                        + "     , grantor.AUTH_ID "
                        + "     , grantee.AUTH_ID ")) {
            dbStat.setString(1, tableId);

            long ownerId = 0;
            long grantorID = 0;
            String grantorName = "";
            long granteeID = 0;
            String granteeName = "";
            String schemaName = "";
            String relName = "";
            long columnId = 0;
            String columnName = "";
            long privType = 0;
            String privName = "";
            String grantable = "";

            long orgColumnId = 0;
            String orgColumnName = "";
            long orgPrivType = 0;
            String orgPrivName = "";

            LinkedList<PrivItem> privItemList = new LinkedList<PrivItem>();

            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    ownerId = dbResult.getLong(1);
                    grantorID = dbResult.getLong(2);
                    grantorName = dbResult.getString(3);
                    granteeID = dbResult.getLong(4);
                    granteeName = dbResult.getString(5);
                    schemaName = dbResult.getString(6);
                    relName = dbResult.getString(7);
                    columnId = dbResult.getLong(8);
                    columnName = dbResult.getString(9);
                    privType = dbResult.getLong(10);
                    privName = dbResult.getString(11);
                    grantable = dbResult.getString(12);

                    orgColumnId = columnId;
                    orgColumnName = columnName;
                    orgPrivType = privType;
                    orgPrivName = privName;

                    privItemList.add(new PrivItem(grantorID, grantorName, granteeID, granteeName,
                            isBuiltIn.contains("YES") ? true : false, grantable.contains("YES") ? true : false));

                    while (dbResult.nextRow()) {
                        ownerId = dbResult.getLong(1);
                        grantorID = dbResult.getLong(2);
                        grantorName = dbResult.getString(3);
                        granteeID = dbResult.getLong(4);
                        granteeName = dbResult.getString(5);
                        schemaName = dbResult.getString(6);
                        relName = dbResult.getString(7);
                        columnId = dbResult.getLong(8);
                        columnName = dbResult.getString(9);
                        privType = dbResult.getLong(10);
                        privName = dbResult.getString(11);
                        grantable = dbResult.getString(12);

                        if ((columnId == orgColumnId) && (orgPrivType == privType)) {
                            privItemList.add(new PrivItem(grantorID, grantorName, granteeID, granteeName,
                                    isBuiltIn.contains("YES") ? true : false,
                                    grantable.contains("YES") ? true : false));
                        } else {
                            LinkedList<PrivItem> privOrderList = buildPrivOrder(privItemList, ownerId);
                            GrantRelationDDL = GrantRelationDDL + getGrantByPrivOrder(privOrderList, "COLUMN",
                                    orgPrivName, schemaName, relName, orgColumnName);

                            orgColumnId = columnId;
                            orgColumnName = columnName;
                            orgPrivType = privType;
                            orgPrivName = privName;

                            privItemList.clear();
                            privItemList.add(new PrivItem(grantorID, grantorName, granteeID, granteeName,
                                    isBuiltIn.contains("YES") ? true : false,
                                    grantable.contains("YES") ? true : false));
                        }
                    }

                    LinkedList<PrivItem> privOrderList = buildPrivOrder(privItemList, ownerId);
                    GrantRelationDDL = GrantRelationDDL + getGrantByPrivOrder(privOrderList, "COLUMN", orgPrivName,
                            schemaName, relName, orgColumnName);
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }

        return GrantRelationDDL;
    }

    private String getCommentRelation(GenericDataSource dataSource, JDBCSession session, String tableId)
            throws DBException {
        String commentDDL = "";
        String ownerName = "";
        String schemaName = "";
        String relName = "";
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "        auth.AUTHORIZATION_NAME "
                        + "      , sch.SCHEMA_NAME         "
                        + "      , tab.TABLE_NAME          "
                        + "      , tab.COMMENTS            "
                        + "   FROM "
                        + "        DEFINITION_SCHEMA.TABLES@LOCAL         AS tab "
                        + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL       AS sch "
                        + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL AS auth "
                        + "  WHERE "
                        + "        tab.SCHEMA_ID = sch.SCHEMA_ID "
                        + "    AND tab.OWNER_ID  = auth.AUTH_ID "
                        + "    AND tab.TABLE_ID  = ? ")) {
            dbStat.setString(1, tableId);

            String comment = "";
            boolean nullComment = false;
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    ownerName = dbResult.getString(1);
                    schemaName = dbResult.getString(2);
                    relName = dbResult.getString(3);
                    comment = dbResult.getString(4);
                    nullComment = dbResult.wasNull();

                    if (nullComment == false) {
                        commentDDL = commentDDL
                                + "\nSET SESSION AUTHORIZATION \"" + ownerName + "\"; "
                                + "\nCOMMENT "
                                + "\n    ON TABLE \"" + schemaName + "\".\"" + relName + "\" "
                                + "\n    IS '" + comment + "' "
                                + "\n;\n"
                                + "COMMIT;\n";
                    }
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
        
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "        col.COLUMN_NAME "
                        + "      , col.COMMENTS    "
                        + "   FROM "
                        + "        DEFINITION_SCHEMA.TABLES@LOCAL  AS tab "
                        + "      , DEFINITION_SCHEMA.COLUMNS@LOCAL AS col "
                        + "  WHERE "
                        + "        tab.TABLE_ID = col.TABLE_ID "
                        + "    AND tab.TABLE_ID = ? "
                        + "  ORDER BY "
                        + "        col.PHYSICAL_ORDINAL_POSITION ")) {
            dbStat.setString(1, tableId);

            String columnName = "";
            String comment = "";
            boolean nullComment = false;
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                while (dbResult.nextRow()) {
                    columnName = dbResult.getString(1);
                    comment = dbResult.getString(2);
                    nullComment = dbResult.wasNull();

                    if (nullComment == false) {
                        commentDDL = commentDDL
                                + "\nSET SESSION AUTHORIZATION \"" + ownerName + "\"; "
                                + "\nCOMMENT "
                                + "\n    ON COLUMN \"" + schemaName + "\".\"" + relName + "\".\"" + columnName + "\" "
                                + "\n    IS '" + comment + "' "
                                + "\n;\n"
                                + "COMMIT;\n";
                    }
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }

        return commentDDL;
    }

    @Override
    public String getTableDDL(DBRProgressMonitor monitor, GenericTableBase sourceObject, Map<String, Object> options)
            throws DBException {
        GenericDataSource dataSource = sourceObject.getDataSource();

        try (JDBCSession session = DBUtils.openMetaSession(monitor, sourceObject, "Read Goldilocks table source")) {
            GoldilocksTable table = (GoldilocksTable)sourceObject;
            String tableId = table.getTableId();
            String tableType = sourceObject.getTableType().toUpperCase(Locale.ENGLISH);

            String tableDDL = getCreateTable(dataSource, session, tableId, tableType)
                    + getTableConstraint(dataSource, session, tableId)
                    + getTableGlobalSeconaryIndex(dataSource, session, tableId)
                    + getTableCreateIndex(dataSource, session, tableId)
                    + getTableIdentity(dataSource, session, tableId);

            if (!tableType.contains("GLOBAL TEMPORARY")) {
                tableDDL = tableDDL + getTableSupplemental(dataSource, session, tableId);
            }

            tableDDL = tableDDL + getGrantRelation(dataSource, session, tableId)
                    + getCommentRelation(dataSource, session, tableId);

            return tableDDL;
        }
    }

    private String getViewId(GenericDataSource dataSource, JDBCSession session, String schemaName, String viewName)
            throws DBException {
        String viewId = "";
        
        try (JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "       tab.TABLE_ID "
                        + "  FROM "
                        + "       DEFINITION_SCHEMA.TABLES@LOCAL tab "
                        + "     , DEFINITION_SCHEMA.SCHEMATA@LOCAL sch "
                        + " WHERE "
                        + "       tab.SCHEMA_ID   = sch.SCHEMA_ID "
                        + "   AND tab.TABLE_TYPE  = 'VIEW' "
                        + "   AND tab.TABLE_NAME  = ? "
                        + "   AND sch.SCHEMA_NAME = ? ")) {
            dbStat.setString(1, viewName);
            dbStat.setString(2, schemaName);
            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    viewId = dbResult.getString(1);
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
        return viewId;
    }

    private String getCreateView(GenericDataSource dataSource, JDBCSession session, String viewId) throws DBException {
        String viewString = "";
        try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                + "        auth.AUTHORIZATION_NAME   "
                + "      , sch.SCHEMA_NAME           "
                + "      , tab.TABLE_NAME            "
                + "      , SPLIT_PART( SPLIT_PART( vew.VIEW_COLUMNS, ')', 1 ), '(', 2 ) "
                + "      , vew.VIEW_DEFINITION       "
                + "   FROM "
                + "        DEFINITION_SCHEMA.TABLES@LOCAL           AS tab  "
                + "      , DEFINITION_SCHEMA.VIEWS@LOCAL            AS vew  "
                + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL         AS sch  "
                + "      , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL   AS auth  "
                + "  WHERE "
                + "        tab.TABLE_ID  = vew.TABLE_ID "
                + "    AND tab.SCHEMA_ID = sch.SCHEMA_ID "
                + "    AND tab.OWNER_ID  = auth.AUTH_ID "
                + "    AND tab.TABLE_ID  = ? ")) {
            dbStat.setString(1, viewId);

            String ownerName = "";
            String schemaName = "";
            String viewName = "";
            String viewColumnLong = "";
            boolean nullViewColumnLong = false;
            String viewSelectLong = "";

            try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                if (dbResult.nextRow()) {
                    ownerName = dbResult.getString(1);
                    schemaName = dbResult.getString(2);
                    viewName = dbResult.getString(3);
                    viewColumnLong = dbResult.getString(4);
                    nullViewColumnLong = dbResult.wasNull();
                    viewSelectLong = dbResult.getString(5);
                }
            }
            
            viewString = viewString
                    + "\nSET SESSION AUTHORIZATION \"" + ownerName + "\"; "
                    + "\nCREATE OR REPLACE FORCE VIEW \"" + schemaName + "\".\"" + viewName + "\" ";
            
            if (nullViewColumnLong == false) {
                viewString = viewString
                        + "\n    (" + viewColumnLong +") ";
            }
            
            viewString = viewString
                    + "\n    AS " + viewSelectLong
                    + "\n;\n"
                    + "COMMIT;\n";

            return viewString;
        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
    }

    @Override
    public String getViewDDL(DBRProgressMonitor monitor, GenericView sourceObject, Map<String, Object> options)
            throws DBException {
        GenericDataSource dataSource = sourceObject.getDataSource();

        try (JDBCSession session = DBUtils.openMetaSession(monitor, sourceObject, "Read Goldilocks view source")) {
            String viewId = getViewId(dataSource, session, sourceObject.getSchema().getName(), sourceObject.getName());

            return getCreateView(dataSource, session, viewId) + getGrantRelation(dataSource, session, viewId)
                    + getCommentRelation(dataSource, session, viewId);
        }
    }

    @Override
    public boolean supportsTableDDLSplit(GenericTableBase sourceObject) {
        return false;
    }
    
    //////////////////////////////////////////////////////
    // Constraints

    @Override
    public JDBCStatement prepareUniqueConstraintsLoadStatement(@NotNull JDBCSession session,
            @NotNull GenericStructContainer owner, @Nullable GenericTableBase forParent)
            throws SQLException, DBException {

        JDBCPreparedStatement dbStat;
        dbStat = session.prepareStatement("SELECT "
                + "        con.CONSTRAINT_ID "
                + "      , con.CONSTRAINT_NAME as PK_NAME " 
                + "      , con.CONSTRAINT_TYPE "  
                + "      , con.ASSOCIATED_INDEX_ID " 
                + "      , con.COMMENTS "
                + "    FROM "  
                + "        DEFINITION_SCHEMA.TABLE_CONSTRAINTS@LOCAL AS con "
                + "      , DEFINITION_SCHEMA.SCHEMATA@LOCAL          AS sch "
                + "      , DEFINITION_SCHEMA.TABLES@LOCAL            AS tab "
                + "   WHERE " 
                + "        con.IS_BUILTIN           = FALSE " 
                + "    AND con.CONSTRAINT_SCHEMA_ID = sch.SCHEMA_ID " 
                + "    AND con.TABLE_ID             = tab.TABLE_ID " 
                + "    AND tab.IS_DROPPED           = FALSE " 
                + "    AND sch.SCHEMA_NAME          = ? "
                + (forParent != null ? "AND tab.TABLE_NAME = ?" : "")
                + "  ORDER BY "
                + "        con.CONSTRAINT_ID ");
        dbStat.setString(1, owner.getName());
        if (forParent != null) {
            dbStat.setString(2, forParent.getName());
        }
        return dbStat;
    }

    @Override
    public DBSEntityConstraintType getUniqueConstraintType(JDBCResultSet dbResult) throws DBException, SQLException {
        String type = JDBCUtils.safeGetString(dbResult, "CONSTRAINT_TYPE");
        if (CommonUtils.isNotEmpty(type)) {
            if ("UNIQUE".equals(type)) {
                return DBSEntityConstraintType.UNIQUE_KEY;
            }
            if ("NOT NULL".equals(type)) {
                return DBSEntityConstraintType.NOT_NULL;
            }
            return DBSEntityConstraintType.PRIMARY_KEY;
        }
        return super.getUniqueConstraintType(dbResult);
    }

    @Override
    public GenericUniqueKey createConstraintImpl(GenericTableBase table, String constraintName,
            DBSEntityConstraintType constraintType, JDBCResultSet dbResult, boolean persisted) {
        return new GenericUniqueKey(table, constraintName, JDBCUtils.safeGetString(dbResult, "COMMENTS"),
                constraintType, persisted);
    }

    @Override
    public GenericTableConstraintColumn[] createConstraintColumnsImpl(JDBCSession session, GenericTableBase parent,
            GenericUniqueKey object, GenericMetaObject pkObject, JDBCResultSet dbResult) throws DBException {
        List<GenericTableConstraintColumn> constraintColumns = new ArrayList<>();

        String constId = JDBCUtils.safeGetString(dbResult, "CONSTRAINT_ID");
        String constType = JDBCUtils.safeGetString(dbResult, "CONSTRAINT_TYPE");
        if (constType.contains("NOT NULL")) {
            try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                    + "        col.COLUMN_NAME "
                    + "   FROM "
                    + "        DEFINITION_SCHEMA.TABLE_CONSTRAINTS@LOCAL  AS cst "
                    + "      , DEFINITION_SCHEMA.CHECK_COLUMN_USAGE@LOCAL AS ccu "
                    + "      , DEFINITION_SCHEMA.COLUMNS@LOCAL            AS col "
                    + "  WHERE      "
                    + "        cst.CONSTRAINT_ID = ccu.CONSTRAINT_ID "
                    + "    AND ccu.COLUMN_ID     = col.COLUMN_ID "
                    + "    AND cst.CONSTRAINT_ID = ? ")) {
                dbStat.setString(1, constId);
                String columnName = "";
                try (JDBCResultSet dbColResult = dbStat.executeQuery()) {
                    while (dbColResult.nextRow()) {
                        columnName = dbColResult.getString(1);

                        GenericTableColumn tableColumn = parent.getAttribute(session.getProgressMonitor(), columnName);
                        if (tableColumn == null) {
                            log.warn("Column '" + columnName + "' not found in table '"
                                    + parent.getFullyQualifiedName(DBPEvaluationContext.DDL) + "'");
                            return null;
                        }

                        constraintColumns.add(new GenericTableConstraintColumn(object, tableColumn, 0));
                    }
                }
            } catch (SQLException e) {
                throw new DBException(e, session.getDataSource());
            }
        } else {
            try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT "
                    + "        col.COLUMN_NAME "
                    + "      , ikey.ORDINAL_POSITION "
                    + "   FROM "
                    + "        DEFINITION_SCHEMA.INDEX_KEY_COLUMN_USAGE@LOCAL AS ikey "
                    + "      , DEFINITION_SCHEMA.TABLE_CONSTRAINTS@LOCAL      AS cst  "
                    + "      , DEFINITION_SCHEMA.COLUMNS@LOCAL                AS col  "
                    + "  WHERE      "
                    + "        cst.ASSOCIATED_INDEX_ID = ikey.INDEX_ID  "
                    + "    AND ikey.COLUMN_ID          = col.COLUMN_ID "
                    + "    AND cst.CONSTRAINT_ID       = ? "
                    + "  ORDER BY "
                    + "        ikey.ORDINAL_POSITION ")) {
                dbStat.setString(1, constId);
                String columnName = "";
                int ordinalPosition = 0;

                try (JDBCResultSet dbColResult = dbStat.executeQuery()) {
                    while (dbColResult.nextRow()) {
                        columnName = dbColResult.getString(1);
                        ordinalPosition = dbColResult.getInt(2);

                        GenericTableColumn tableColumn = parent.getAttribute(session.getProgressMonitor(), columnName);
                        if (tableColumn == null) {
                            log.warn("Column '" + columnName + "' not found in table '"
                                    + parent.getFullyQualifiedName(DBPEvaluationContext.DDL) + "'");
                            return null;
                        }

                        constraintColumns.add(new GenericTableConstraintColumn(object, tableColumn, ordinalPosition));
                    }
                }
            } catch (SQLException e) {
                throw new DBException(e, session.getDataSource());
            }
        }

        return ArrayUtils.toArray(GenericTableConstraintColumn.class, constraintColumns);
    }

    //////////////////////////////////////////////////////
    // Indexes

    @Override
    public GenericTableIndex createIndexImpl(GenericTableBase table, boolean nonUnique, String qualifier,
            long cardinality, String indexName, DBSIndexType indexType, boolean persisted) {
        return new GoldilocksIndex((GoldilocksTable)table, nonUnique, qualifier, cardinality, indexName, persisted, "", "", "");
    }

    //////////////////////////////////////////////////////
    // Sequences

    @Override
    public boolean supportsSequences(@NotNull GenericDataSource dataSource) {
        return true;
    }

    @Override
    public JDBCStatement prepareSequencesLoadStatement(@NotNull JDBCSession session,
            @NotNull GenericStructContainer container) throws SQLException {
        JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "      SEQUENCE_NAME "
                        + "   , sqc.COMMENTS "
                        + "   , MINIMUM_VALUE "
                        + "   , MAXIMUM_VALUE "
                        + "   , INCREMENT "
                        + "FROM "
                        + "     DEFINITION_SCHEMA.SEQUENCES@LOCAL AS sqc "
                        + "   , DEFINITION_SCHEMA.SCHEMATA@LOCAL  AS sch "
                        + "WHERE "
                        + "     sqc.IS_BUILTIN  = FALSE "
                        + " AND sqc.SCHEMA_ID   = sch.SCHEMA_ID "
                        + " AND sch.SCHEMA_NAME = ? "
                        + "ORDER BY "
                        + "     SEQUENCE_NAME");
        dbStat.setString(1, container.getName());
        return dbStat;
    }

    @Override
    public GenericSequence createSequenceImpl(@NotNull JDBCSession session, @NotNull GenericStructContainer container,
            @NotNull JDBCResultSet dbResult) {
        String name = JDBCUtils.safeGetString(dbResult, 1);
        if (CommonUtils.isEmpty(name)) {
            return null;
        }
        String description = JDBCUtils.safeGetString(dbResult, 2);
        Number minValue = JDBCUtils.safeGetBigDecimal(dbResult, 3);
        Number maxValue = JDBCUtils.safeGetBigDecimal(dbResult, 4);
        Number incrementBy = JDBCUtils.safeGetBigDecimal(dbResult, 5);
        return new GenericSequence(container, name, description, null, minValue, maxValue, incrementBy);
    }

    //////////////////////////////////////////////////////
    // Synonyms

    @Override
    public boolean supportsSynonyms(@NotNull GenericDataSource dataSource) {
        return true;
    }

    @Override
    public JDBCStatement prepareSynonymsLoadStatement(@NotNull JDBCSession session,
            @NotNull GenericStructContainer container) throws SQLException {
        JDBCPreparedStatement dbStat = session
                .prepareStatement("SELECT "
                        + "     syn.SYNONYM_NAME "
                        + "   , syn.OBJECT_SCHEMA_NAME "
                        + "   , syn.OBJECT_NAME "
                        + "FROM "
                        + "     DEFINITION_SCHEMA.SYNONYMS@LOCAL       AS syn "
                        + "   , DEFINITION_SCHEMA.SCHEMATA@LOCAL       AS sch "
                        + "   , DEFINITION_SCHEMA.AUTHORIZATIONS@LOCAL AS auth "
                        + "WHERE "
                        + "     syn.SCHEMA_ID   = sch.SCHEMA_ID "
                        + " AND syn.OWNER_ID    = auth.AUTH_ID "
                        + " AND sch.SCHEMA_NAME = ?"
                        + "ORDER BY "
                        + "     syn.SYNONYM_NAME");
        dbStat.setString(1, container.getName());
        return dbStat;
    }

    @Override
    public GenericSynonym createSynonymImpl(@NotNull JDBCSession session, @NotNull GenericStructContainer container,
            @NotNull JDBCResultSet dbResult) {
        String name = JDBCUtils.safeGetString(dbResult, 1);
        if (CommonUtils.isEmpty(name)) {
            return null;
        }
        String targetObjectSchema = JDBCUtils.safeGetString(dbResult, 2);
        String targetObjectName = JDBCUtils.safeGetString(dbResult, 3);
        return new GoldilocksSynonym(container, name, targetObjectSchema, targetObjectName);
    }
}
