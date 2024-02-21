package org.jkiss.dbeaver.ext.goldilocks.model;

import java.util.Map;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericCatalog;
import org.jkiss.dbeaver.ext.generic.model.GenericPackage;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.model.DBPScriptObject;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.utils.CommonUtils;

public class GoldilocksPackage extends GenericPackage {

    private String ddl;

    public GoldilocksPackage(GenericStructContainer container, String packageName, boolean nameFromCatalog) {
        super(container, packageName, nameFromCatalog);
    }

    @Property(hidden = true)
    public GenericCatalog getCatalog() {
        return super.getCatalog();
    }

    @Override
    public String getObjectDefinitionText(DBRProgressMonitor monitor, Map<String, Object> options) throws DBException {
        if (CommonUtils.getOption(options, DBPScriptObject.OPTION_REFRESH)) {
            ddl = null;
        }
        if (ddl == null) {
            ddl = ((GoldilocksMetaModel) getDataSource().getMetaModel()).getPackageDDL(monitor, this);
        }
        return ddl;
    }
}
