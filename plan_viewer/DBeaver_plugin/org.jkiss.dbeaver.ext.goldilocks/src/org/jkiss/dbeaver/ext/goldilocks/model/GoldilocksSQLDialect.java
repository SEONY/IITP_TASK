package org.jkiss.dbeaver.ext.goldilocks.model;

import org.jkiss.dbeaver.ext.generic.model.GenericSQLDialect;

public class GoldilocksSQLDialect extends GenericSQLDialect {

    public GoldilocksSQLDialect() {
        super("Goldilocks SQL", "goldilocks");
    }
    
    @Override
    public boolean isDelimiterAfterBlock() {
        return true;
    }
}
