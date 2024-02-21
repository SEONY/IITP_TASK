package org.jkiss.dbeaver.ext.goldilocks.model;

import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericView;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.impl.DBObjectNameCaseTransformer;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.meta.PropertyLength;

public class GoldilocksView extends GenericView {

    public GoldilocksView(GenericStructContainer container, String tableName, String tableType,
            JDBCResultSet dbResult) {
        super(container, tableName, tableType, dbResult);
    }

    @Property(viewable = true, editable = true, valueTransformer = DBObjectNameCaseTransformer.class, order = 1)
    public String getName() {
        return super.getName();
    }

    @Property(viewable = true, editableExpr = "object.dataSource.metaModel.tableCommentEditable", updatableExpr = "object.dataSource.metaModel.tableCommentEditable", length = PropertyLength.MULTILINE, order = 100)
    public String getDescription() {
        return super.getDescription();
    }

    @Property(hidden = true)
    public String getTableType() {
        return super.getTableType();
    }
}
