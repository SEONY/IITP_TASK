package org.jkiss.dbeaver.ext.goldilocks.model.plan;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.plan.DBCPlanNode;
import org.jkiss.dbeaver.model.impl.plan.AbstractExecutionPlan;

public class GoldilocksPlanAnalyser extends AbstractExecutionPlan {

    private final int EXPLAIN_PLAN_ONLY = 3;

    private JDBCSession session;
    private String query;
    private List<GoldilocksPlanNode> rootNodes;

    public GoldilocksPlanAnalyser(JDBCSession session, String query) {
        this.session = session;
        this.query = query;
    }

    public void explain() throws DBException {
        try {
            JDBCPreparedStatement dbStat = session.prepareStatement(getQueryString());

            try {
                dbStat.getOriginal().getClass().getMethod("setExplainPlanOption", int.class)
                        .invoke(dbStat.getOriginal(), EXPLAIN_PLAN_ONLY);
                dbStat.execute();

                String plan = (String) dbStat.getOriginal().getClass().getMethod("getExplainPlan")
                        .invoke(dbStat.getOriginal());
                GoldilocksPlanBuilder builder = new GoldilocksPlanBuilder(plan);
                rootNodes = builder.Build();
            } catch (Exception e) {
                throw new DBCException(e, session.getExecutionContext());
            } finally {
                dbStat.close();
            }
        } catch (SQLException e) {
            throw new DBCException(e, session.getExecutionContext());
        }
    }

    @Override
    public String getQueryString() {
        return query;
    }

    @Override
    public String getPlanQueryString() throws DBException {
        return null;
    }

    @Override
    public List<? extends DBCPlanNode> getPlanNodes(Map<String, Object> options) {
        return rootNodes;
    }
}
