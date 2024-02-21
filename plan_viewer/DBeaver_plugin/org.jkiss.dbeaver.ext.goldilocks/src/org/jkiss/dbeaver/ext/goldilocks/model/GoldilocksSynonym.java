package org.jkiss.dbeaver.ext.goldilocks.model;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericSynonym;
import org.jkiss.dbeaver.model.DBPQualifiedObject;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;

public class GoldilocksSynonym extends GenericSynonym implements DBPQualifiedObject {

    private String targetObjectSchema;
    private String targetObjectName;

    protected GoldilocksSynonym(GenericStructContainer container, String name, String targetObjectSchema,
            String targetObjectName) {
        super(container, name, null);
        this.targetObjectSchema = targetObjectSchema;
        this.targetObjectName = targetObjectName;
    }

    @Property(viewable = true, order = 20)
    public DBSObject getTargetObjectSchema(DBRProgressMonitor monitor) throws DBException {
        return getDataSource().getSchema(targetObjectSchema);
    }

    @Override
    @Property(viewable = true, order = 21)
    public DBSObject getTargetObject(DBRProgressMonitor monitor) throws DBException {
        GenericSchema schema = getDataSource().getSchema(targetObjectSchema);

        DBSObject object = schema.getProcedure(monitor, targetObjectName);

        if (object == null) {
            object = schema.getChild(monitor, targetObjectName);
        }

        return object;
    }
}
