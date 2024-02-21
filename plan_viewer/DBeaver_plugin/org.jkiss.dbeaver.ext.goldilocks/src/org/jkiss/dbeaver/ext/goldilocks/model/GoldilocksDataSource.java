package org.jkiss.dbeaver.ext.goldilocks.model;

import java.sql.CallableStatement;
import java.sql.SQLException;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaModel;
import org.jkiss.dbeaver.ext.goldilocks.model.plan.GoldilocksPlanAnalyser;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.exec.DBCExecutionPurpose;
import org.jkiss.dbeaver.model.exec.DBCExecutionResult;
import org.jkiss.dbeaver.model.exec.output.DBCOutputWriter;
import org.jkiss.dbeaver.model.exec.output.DBCServerOutputReader;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.DBCStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.plan.DBCPlan;
import org.jkiss.dbeaver.model.exec.plan.DBCPlanStyle;
import org.jkiss.dbeaver.model.exec.plan.DBCQueryPlanner;
import org.jkiss.dbeaver.model.exec.plan.DBCQueryPlannerConfiguration;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCExecutionContext;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

public class GoldilocksDataSource extends GenericDataSource implements DBCQueryPlanner {

    private GoldilocksOutputReader outputReader;

    public GoldilocksDataSource(DBRProgressMonitor monitor, DBPDataSourceContainer container,
            GenericMetaModel metaModel) throws DBException {
        super(monitor, container, metaModel, new GoldilocksSQLDialect());
        this.outputReader = new GoldilocksOutputReader();
    }

    @Override
    protected void initializeContextState(@NotNull DBRProgressMonitor monitor, @NotNull JDBCExecutionContext context,
            JDBCExecutionContext initFrom) throws DBException {
        super.initializeContextState(monitor, context, initFrom);

        if (outputReader == null) {
            outputReader = new GoldilocksOutputReader();
        }
        // Enable DBMS output
        outputReader.enableServerOutput(monitor, context, outputReader.isServerOutputEnabled());
    }

    @Override
    public boolean isOmitCatalog() {
        return true;
    }

    @Nullable
    @Override
    public <T> T getAdapter(Class<T> adapter) {
        if (adapter == DBCServerOutputReader.class) {
            return adapter.cast(outputReader);
        }
        return super.getAdapter(adapter);
    }

    @Override
    public DBCPlan planQueryExecution(DBCSession session, String query, DBCQueryPlannerConfiguration configuration)
            throws DBException {
        GoldilocksPlanAnalyser plan = new GoldilocksPlanAnalyser((JDBCSession) session, query);
        plan.explain();

        return plan;
    }

    @Override
    public DBCPlanStyle getPlanStyle() {
        return DBCPlanStyle.PLAN;
    }

    private class GoldilocksOutputReader implements DBCServerOutputReader {
        @Override
        public boolean isServerOutputEnabled() {
            return true;
        }

        @Override
        public boolean isAsyncOutputReadSupported() {
            return false;
        }

        public void enableServerOutput(DBRProgressMonitor monitor, DBCExecutionContext context, boolean enable)
                throws DBCException {
            String sql = enable ?
                    "BEGIN DBMS_OUTPUT.ENABLE(NULL); END;" :
                    "BEGIN DBMS_OUTPUT.DISABLE; END;";
            try (DBCSession session = context.openSession(monitor, DBCExecutionPurpose.UTIL,
                    (enable ? "Enable" : "Disable ") + "DBMS output")) {
                JDBCUtils.executeSQL((JDBCSession) session, sql);
            } catch (SQLException e) {
                throw new DBCException(e, context);
            }
        }

        @Override
        public void readServerOutput(@NotNull DBRProgressMonitor monitor, @NotNull DBCExecutionContext context,
                @Nullable DBCExecutionResult executionResult, @Nullable DBCStatement statement,
                @NotNull DBCOutputWriter output) throws DBCException {
            try (JDBCSession session = (JDBCSession) context.openSession(monitor, DBCExecutionPurpose.UTIL,
                    "Read DBMS output")) {
                try (CallableStatement getLineProc = session.getOriginal()
                        .prepareCall("{CALL DBMS_OUTPUT.GET_LINE(?, ?)}")) {
                    getLineProc.registerOutParameter(1, java.sql.Types.VARCHAR);
                    getLineProc.registerOutParameter(2, java.sql.Types.INTEGER);
                    int status = 0;
                    while (status == 0) {
                        getLineProc.execute();
                        status = getLineProc.getInt(2);
                        if (status == 0) {
                            output.println(null, getLineProc.getString(1));
                        }
                    }
                } catch (SQLException e) {
                    throw new DBCException(e, context);
                }
            }
        }
    }
}
