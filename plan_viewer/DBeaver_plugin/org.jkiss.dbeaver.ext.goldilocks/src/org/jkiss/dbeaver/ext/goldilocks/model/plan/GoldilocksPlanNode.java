package org.jkiss.dbeaver.ext.goldilocks.model.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jkiss.dbeaver.model.exec.plan.DBCPlanCostNode;
import org.jkiss.dbeaver.model.exec.plan.DBCPlanNode;
import org.jkiss.dbeaver.model.impl.PropertyDescriptor;
import org.jkiss.dbeaver.model.impl.plan.AbstractExecutionPlanNode;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.preferences.DBPPropertyDescriptor;
import org.jkiss.dbeaver.model.preferences.DBPPropertySource;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.utils.IntKeyMap;

public class GoldilocksPlanNode extends AbstractExecutionPlanNode implements DBCPlanCostNode, DBPPropertySource {

    private GoldilocksPlanNode parent;
    private String operation;
    private Map<String, String> nodeProps = new LinkedHashMap<>();
    private List<GoldilocksPlanNode> nested = new ArrayList<>();

    public GoldilocksPlanNode(IntKeyMap<GoldilocksPlanNode> levelNodes, int level, int idx, String operation) {
        this.operation = operation;

        addProperty("IDX", String.valueOf(idx));

        if (level > 0) {
            parent = levelNodes.get(level - 1);
        }

        if (parent != null) {
            parent.addChild(this);
        }
    }

    private void addChild(GoldilocksPlanNode node) {
        this.nested.add(node);
    }

    public void addProperty(String key, String value) {
        nodeProps.put(key, value);
    }

    @Override
    public String getNodeName() {
        return null;
    }

    @Override
    public String getNodeType() {
        return operation;
    }

    @Override
    public DBCPlanNode getParent() {
        return parent;
    }

    @Override
    public Collection<? extends DBCPlanNode> getNested() {
        return nested;
    }

    @Override
    public Object getEditableValue() {
        return this;
    }

    @Override
    public DBPPropertyDescriptor[] getProperties() {
        DBPPropertyDescriptor[] props = new DBPPropertyDescriptor[nodeProps.size()];
        int index = 0;
        for (Map.Entry<String, String> attr : nodeProps.entrySet()) {
            props[index++] = new PropertyDescriptor("Details", attr.getKey(), attr.getKey(), null, String.class, false,
                    null, null, false);
        }
        return props;
    }

    @Override
    public Object getPropertyValue(DBRProgressMonitor monitor, String id) {
        return nodeProps.get(id.toString());
    }

    @Override
    public boolean isPropertySet(String id) {
        return nodeProps.containsKey(id.toString());
    }

    @Override
    public boolean isPropertyResettable(String id) {
        return false;
    }

    @Override
    public void resetPropertyValue(DBRProgressMonitor monitor, String id) {
    }

    @Override
    public void resetPropertyValueToDefault(String id) {
    }

    @Override
    public void setPropertyValue(DBRProgressMonitor monitor, String id, Object value) {
    }

    @Override
    public Number getNodeCost() {
        return null;
    }

    @Override
    public Number getNodePercent() {
        return null;
    }

    @Override
    public Number getNodeDuration() {
        return null;
    }

    @Override
    public Number getNodeRowCount() {
        return null;
    }

    @Property(order = 1, viewable = true)
    public String getOperation() {
        return operation;
    }

    @Override
    public String toString() {
        return operation;
    }
}
