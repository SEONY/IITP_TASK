package org.jkiss.dbeaver.ext.goldilocks.model;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.GenericConstants;
import org.jkiss.dbeaver.ext.generic.model.GenericCatalog;
import org.jkiss.dbeaver.ext.generic.model.GenericFunctionResultType;
import org.jkiss.dbeaver.ext.generic.model.GenericProcedure;
import org.jkiss.dbeaver.ext.generic.model.GenericProcedureParameter;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericUtils;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaObject;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCConstants;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureParameterKind;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;
import org.jkiss.utils.CommonUtils;

public class GoldilocksProcedure extends GenericProcedure {

    public GoldilocksProcedure(GenericStructContainer container, String procedureName, String specificName,
            String description, DBSProcedureType procedureType, GenericFunctionResultType functionResultType) {
        super(container, procedureName, specificName, description, procedureType, functionResultType);
    }

    @Override
    public void loadProcedureColumns(DBRProgressMonitor monitor) throws DBException {
        Collection<? extends GenericProcedure> procedures = getContainer().getProcedures(monitor, getName());
        if (procedures == null || !procedures.contains(this)) {
            throw new DBException("Internal error - cannot read columns for procedure '" + getName()
                    + "' because its not found in container");
        }
        Iterator<? extends GenericProcedure> procIter = procedures.iterator();
        GenericProcedure procedure = null;

        final GenericMetaObject pcObject = getDataSource().getMetaObject(GenericConstants.OBJECT_PROCEDURE_COLUMN);
        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load procedure columns")) {
            final JDBCResultSet dbResult;
            dbResult = session.getMetaData().getProcedureColumns(getCatalog() == null ? null : getCatalog().getName(),
                    getSchema() == null ? null : JDBCUtils.escapeWildCards(session, getSchema().getName()),
                    JDBCUtils.escapeWildCards(session,
                            this.getPackage() == null ? getName() : this.getPackage().getName() + "." + getName()),
                    getDataSource().getAllObjectsPattern());
            try {
                int previousPosition = -1;
                while (dbResult.next()) {
                    String columnName = GenericUtils.safeGetString(pcObject, dbResult, JDBCConstants.COLUMN_NAME);
                    int columnTypeNum = GenericUtils.safeGetInt(pcObject, dbResult, JDBCConstants.COLUMN_TYPE);
                    int valueType = GenericUtils.safeGetInt(pcObject, dbResult, JDBCConstants.DATA_TYPE);
                    String typeName = GenericUtils.safeGetString(pcObject, dbResult, JDBCConstants.TYPE_NAME);
                    int columnSize = GenericUtils.safeGetInt(pcObject, dbResult, JDBCConstants.LENGTH);
                    boolean notNull = GenericUtils.safeGetInt(pcObject, dbResult,
                            JDBCConstants.NULLABLE) == DatabaseMetaData.procedureNoNulls;
                    int scale = GenericUtils.safeGetInt(pcObject, dbResult, JDBCConstants.SCALE);
                    int precision = GenericUtils.safeGetInt(pcObject, dbResult, JDBCConstants.PRECISION);
                    // int radix = GenericUtils.safeGetInt(dbResult, JDBCConstants.RADIX);
                    String remarks = GenericUtils.safeGetString(pcObject, dbResult, JDBCConstants.REMARKS);
                    int position = GenericUtils.safeGetInt(pcObject, dbResult, JDBCConstants.ORDINAL_POSITION);
                    DBSProcedureParameterKind parameterType;
                    switch (columnTypeNum) {
                    case DatabaseMetaData.procedureColumnIn:
                        parameterType = DBSProcedureParameterKind.IN;
                        break;
                    case DatabaseMetaData.procedureColumnInOut:
                        parameterType = DBSProcedureParameterKind.INOUT;
                        break;
                    case DatabaseMetaData.procedureColumnOut:
                        parameterType = DBSProcedureParameterKind.OUT;
                        break;
                    case DatabaseMetaData.procedureColumnReturn:
                        parameterType = DBSProcedureParameterKind.RETURN;
                        break;
                    case DatabaseMetaData.procedureColumnResult:
                        parameterType = DBSProcedureParameterKind.RESULTSET;
                        break;
                    default:
                        parameterType = DBSProcedureParameterKind.UNKNOWN;
                        break;
                    }

                    if (CommonUtils.isEmpty(columnName) && parameterType == DBSProcedureParameterKind.RETURN) {
                        columnName = "RETURN";
                    }
                    if (procedure == null
                            || (previousPosition >= 0 && position <= previousPosition && procIter.hasNext())) {
                        procedure = procIter.next();
                    }
                    GenericProcedureParameter column = new GenericProcedureParameter(procedure, columnName, typeName,
                            valueType, position, columnSize, scale, precision, notNull, remarks, parameterType);

                    procedure.addColumn(column);

                    previousPosition = position;
                }
            } finally {
                dbResult.close();
            }
        } catch (SQLException e) {
            throw new DBException(e, getDataSource());
        }
    }

    @Property(hidden = true)
    public GenericCatalog getCatalog() {
        return super.getCatalog();
    }

    @Property(hidden = true)
    public GenericFunctionResultType getFunctionResultType() {
        return super.getFunctionResultType();
    }
}
